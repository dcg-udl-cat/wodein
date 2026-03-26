package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const marketDateLayout = "02-01-2006"

var fullAuctionIDPattern = regexp.MustCompile(`^(\d{2}-\d{2}-\d{4})-A([123])$`)

type Config struct {
	DatasetPath          string
	DatasetsRoot         string
	RunAll               bool
	ResultsPath          string
	SummaryPath          string
	APIBaseURL           string
	Timezone             string
	RequestTimeout       time.Duration
	PerTransactionBudget time.Duration
	LogLevel             slog.Level
}

type DatasetRow struct {
	RowNumber           int
	SubmissionRaw       string
	SubmissionTS        time.Time
	AuctionSession      string
	ParticipantID       string
	OrderType           string
	Period              int
	DatasetOrderID      string
	CanonicalBlocksJSON string
	NumBlocks           int
	AuctionRef          AuctionRef
}

type AuctionRef struct {
	MarketID      string
	AuctionID     string
	SessionNumber int
	SessionLabel  string
}

type ControllerAction string

const (
	ActionOpenMarket   ControllerAction = "open_market"
	ActionOpenAuction  ControllerAction = "open_auction"
	ActionCloseAuction ControllerAction = "close_auction"
)

type ControllerCommand struct {
	Action       ControllerAction
	MarketID     string
	AuctionID    string
	SubmissionTS time.Time
	Reply        chan ControllerAck
}

type ControllerAck struct {
	Result BenchmarkResult
	Err    error
}

type BenchmarkResult struct {
	Index               int
	Kind                string
	SubmissionTS        string
	BatchKey            string
	ParticipantID       string
	FabricUser          string
	AuctionSession      string
	MarketID            string
	AuctionID           string
	DatasetOrderID      string
	ChaincodeOrderID    string
	Action              string
	RowsInBatch         int
	ExecutionDurationMS float64
	Status              string
	Error               string
}

type BenchmarkSummary struct {
	DatasetPath  string
	ResultsPath  string
	Rows         int
	Results      int
	Succeeded    int
	Failed       int
	DurationMS   float64
	Status       string
	ErrorMessage string
}

type ResultStore struct {
	mu      sync.Mutex
	nextIdx int
	results []BenchmarkResult
}

type APIClient struct {
	baseURL string
	http    *http.Client
}

type Orchestrator struct {
	cfg          Config
	loc          *time.Location
	controllerCh chan<- ControllerCommand
	api          *APIClient
	results      *ResultStore
}

type APIBlock struct {
	Amount int     `json:"amount"`
	Price  float64 `json:"price"`
}

type addOrderRequest struct {
	IDAuction string     `json:"id_auction"`
	Period    int        `json:"period"`
	Type      string     `json:"type"`
	Blocks    []APIBlock `json:"blocks"`
}

type addOrderResponse struct {
	OrderID string `json:"order_id"`
}

type apiError struct {
	Error string `json:"error"`
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		logger.Error("failed to load timezone", "timezone", cfg.Timezone, "error", err)
		os.Exit(1)
	}

	api := &APIClient{
		baseURL: strings.TrimRight(cfg.APIBaseURL, "/"),
		http:    &http.Client{Timeout: cfg.RequestTimeout},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	summaries, runErr := runBenchmarks(ctx, cfg, loc, api, logger)
	if writeErr := writeSummaryCSV(cfg.SummaryPath, summaries); writeErr != nil {
		logger.Error("failed writing summary", "path", cfg.SummaryPath, "error", writeErr)
		if runErr == nil {
			runErr = writeErr
		}
	}

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		logger.Error("benchmark-db failed", "error", runErr)
		os.Exit(1)
	}

	logger.Info("benchmark-db finished", "datasets", len(summaries), "summary", cfg.SummaryPath)
}

func loadConfig() (Config, error) {
	datasetPath := flag.String("dataset", "", "Path to one dataset CSV")
	datasetsRoot := flag.String("datasets-root", ".", "Root path to discover */dataset.csv")
	runAll := flag.Bool("all", false, "Run benchmarks for all discovered datasets under --datasets-root")
	resultsPath := flag.String("results", "benchmark_results.csv", "Results CSV path (single dataset mode)")
	summaryPath := flag.String("summary", "benchmark_results_summary.csv", "Summary CSV path")
	apiBaseURL := flag.String("api-base-url", "", "DB API base URL")
	flag.Parse()

	requestTimeout, err := parseDurationEnv("BENCH_DB_REQUEST_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	perTxBudget, err := parseDurationEnv("BENCH_DB_PER_TX_BUDGET", 25*time.Second)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		DatasetPath:          strings.TrimSpace(*datasetPath),
		DatasetsRoot:         filepath.Clean(*datasetsRoot),
		RunAll:               *runAll,
		ResultsPath:          filepath.Clean(*resultsPath),
		SummaryPath:          filepath.Clean(*summaryPath),
		APIBaseURL:           strings.TrimSpace(firstNonEmpty(*apiBaseURL, os.Getenv("BENCH_DB_API_BASE_URL"), "http://127.0.0.1:8080")),
		Timezone:             envOrDefault("APP_TIMEZONE", "Europe/Madrid"),
		RequestTimeout:       requestTimeout,
		PerTransactionBudget: perTxBudget,
		LogLevel:             parseLogLevel(envOrDefault("APP_LOG_LEVEL", "INFO")),
	}

	if cfg.DatasetPath == "" && !cfg.RunAll {
		cfg.RunAll = true
	}

	return cfg, nil
}

func runBenchmarks(ctx context.Context, cfg Config, loc *time.Location, api *APIClient, logger *slog.Logger) ([]BenchmarkSummary, error) {
	datasets, err := resolveDatasets(cfg)
	if err != nil {
		return nil, err
	}
	if len(datasets) == 0 {
		return nil, fmt.Errorf("no datasets found")
	}

	summaries := make([]BenchmarkSummary, 0, len(datasets))
	var firstErr error

	for _, entry := range datasets {
		if ctx.Err() != nil {
			return summaries, ctx.Err()
		}

		logger.Info("running dataset benchmark", "dataset", entry.DatasetPath, "results", entry.ResultsPath)
		summary := runSingleDataset(ctx, cfg, loc, api, logger, entry.DatasetPath, entry.ResultsPath)
		summaries = append(summaries, summary)
		if summary.Status != "success" && firstErr == nil {
			firstErr = errors.New(summary.ErrorMessage)
		}
	}

	return summaries, firstErr
}

type datasetEntry struct {
	DatasetPath string
	ResultsPath string
}

func resolveDatasets(cfg Config) ([]datasetEntry, error) {
	if !cfg.RunAll {
		if cfg.DatasetPath == "" {
			return nil, fmt.Errorf("--dataset is required when --all=false")
		}
		return []datasetEntry{{DatasetPath: filepath.Clean(cfg.DatasetPath), ResultsPath: filepath.Clean(cfg.ResultsPath)}}, nil
	}

	entries, err := os.ReadDir(cfg.DatasetsRoot)
	if err != nil {
		return nil, fmt.Errorf("read datasets root: %w", err)
	}

	var datasets []datasetEntry
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		datasetPath := filepath.Join(cfg.DatasetsRoot, entry.Name(), "dataset.csv")
		if _, err := os.Stat(datasetPath); err != nil {
			continue
		}
		datasets = append(datasets, datasetEntry{
			DatasetPath: datasetPath,
			ResultsPath: filepath.Join(cfg.DatasetsRoot, entry.Name(), "benchmark_results.csv"),
		})
	}

	sort.Slice(datasets, func(i, j int) bool {
		return datasets[i].DatasetPath < datasets[j].DatasetPath
	})
	return datasets, nil
}

func runSingleDataset(ctx context.Context, cfg Config, loc *time.Location, api *APIClient, logger *slog.Logger, datasetPath, resultsPath string) BenchmarkSummary {
	started := time.Now()
	rows, err := loadDatasetRows(datasetPath, loc)
	if err != nil {
		return BenchmarkSummary{DatasetPath: datasetPath, ResultsPath: resultsPath, Status: "fail", ErrorMessage: err.Error()}
	}

	results := &ResultStore{}
	controllerCh := make(chan ControllerCommand)
	controllerDone := make(chan error, 1)

	go func() {
		controllerDone <- runController(ctx, api, cfg, controllerCh)
	}()

	orchestrator := &Orchestrator{
		cfg:          cfg,
		loc:          loc,
		controllerCh: controllerCh,
		api:          api,
		results:      results,
	}

	runErr := orchestrator.Run(ctx, rows)
	close(controllerCh)
	controllerErr := <-controllerDone
	if runErr == nil && controllerErr != nil && !errors.Is(controllerErr, context.Canceled) {
		runErr = controllerErr
	}

	snapshot := results.Snapshot()
	if writeErr := writeResultsCSV(resultsPath, snapshot); writeErr != nil {
		if runErr == nil {
			runErr = writeErr
		}
	}

	succeeded, failed := countStatus(snapshot)
	summary := BenchmarkSummary{
		DatasetPath: datasetPath,
		ResultsPath: resultsPath,
		Rows:        len(rows),
		Results:     len(snapshot),
		Succeeded:   succeeded,
		Failed:      failed,
		DurationMS:  durationMillis(time.Since(started)),
		Status:      "success",
	}

	if runErr != nil {
		summary.Status = "fail"
		summary.ErrorMessage = runErr.Error()
		logger.Error("dataset benchmark failed", "dataset", datasetPath, "error", runErr)
	} else {
		logger.Info("dataset benchmark finished", "dataset", datasetPath, "rows", len(rows), "results", len(snapshot), "results_path", resultsPath)
	}

	return summary
}

func countStatus(results []BenchmarkResult) (success int, fail int) {
	for _, result := range results {
		if result.Status == "success" {
			success++
		} else {
			fail++
		}
	}
	return success, fail
}

func runController(ctx context.Context, api *APIClient, cfg Config, commands <-chan ControllerCommand) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case cmd, ok := <-commands:
			if !ok {
				return nil
			}

			started := time.Now()
			actionCtx, cancel := context.WithTimeout(ctx, cfg.PerTransactionBudget)

			var err error
			switch cmd.Action {
			case ActionOpenMarket:
				err = api.EnsureMarket(actionCtx, cmd.MarketID)
			case ActionOpenAuction:
				err = api.OpenAuction(actionCtx, cmd.AuctionID)
			case ActionCloseAuction:
				err = api.CloseAuction(actionCtx, cmd.AuctionID)
			default:
				err = fmt.Errorf("unknown controller action %q", cmd.Action)
			}
			cancel()

			result := BenchmarkResult{
				Kind:                "lifecycle_tx",
				SubmissionTS:        cmd.SubmissionTS.Format(time.RFC3339Nano),
				MarketID:            cmd.MarketID,
				AuctionID:           cmd.AuctionID,
				Action:              string(cmd.Action),
				RowsInBatch:         1,
				ExecutionDurationMS: durationMillis(time.Since(started)),
				Status:              "success",
			}
			if err != nil {
				result.Status = "fail"
				result.Error = err.Error()
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case cmd.Reply <- ControllerAck{Result: result, Err: err}:
			}
		}
	}
}

func (o *Orchestrator) Run(ctx context.Context, rows []DatasetRow) error {
	var active *AuctionRef
	var batch []DatasetRow

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		nextAuction := batch[0].AuctionRef
		if active == nil || active.AuctionID != nextAuction.AuctionID {
			if active != nil {
				if err := o.signalController(ctx, ActionCloseAuction, active.MarketID, active.AuctionID, batch[0].SubmissionTS); err != nil {
					return err
				}
			}
			if active == nil || active.MarketID != nextAuction.MarketID {
				if err := o.signalController(ctx, ActionOpenMarket, nextAuction.MarketID, "", batch[0].SubmissionTS); err != nil {
					return err
				}
			}
			if err := o.signalController(ctx, ActionOpenAuction, nextAuction.MarketID, nextAuction.AuctionID, batch[0].SubmissionTS); err != nil {
				return err
			}
			activeCopy := nextAuction
			active = &activeCopy
		}

		if err := o.executeOrderBatch(ctx, batch); err != nil {
			return err
		}

		batch = nil
		return nil
	}

	for _, row := range rows {
		if len(batch) == 0 {
			batch = append(batch, row)
			continue
		}

		batchSecond := batch[0].SubmissionTS.Truncate(time.Second)
		rowSecond := row.SubmissionTS.Truncate(time.Second)
		sameSecond := rowSecond.Equal(batchSecond)
		sameAuction := row.AuctionRef.AuctionID == batch[0].AuctionRef.AuctionID

		if sameSecond && sameAuction {
			batch = append(batch, row)
			continue
		}

		if err := flushBatch(); err != nil {
			return err
		}
		batch = append(batch, row)
	}

	if err := flushBatch(); err != nil {
		return err
	}

	if active != nil {
		lastTS := rows[len(rows)-1].SubmissionTS
		if err := o.signalController(ctx, ActionCloseAuction, active.MarketID, active.AuctionID, lastTS); err != nil {
			return err
		}
	}

	return nil
}

func (o *Orchestrator) signalController(ctx context.Context, action ControllerAction, marketID, auctionID string, submissionTS time.Time) error {
	reply := make(chan ControllerAck, 1)
	cmd := ControllerCommand{Action: action, MarketID: marketID, AuctionID: auctionID, SubmissionTS: submissionTS, Reply: reply}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case o.controllerCh <- cmd:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case ack := <-reply:
		o.results.Add(ack.Result)
		return ack.Err
	}
}

func (o *Orchestrator) executeOrderBatch(ctx context.Context, batch []DatasetRow) error {
	if len(batch) == 0 {
		return nil
	}

	batchKey := batch[0].SubmissionTS.Truncate(time.Second).Format(time.RFC3339)
	started := time.Now()

	var wg sync.WaitGroup
	resultCh := make(chan BenchmarkResult, len(batch))

	for _, row := range batch {
		row := row
		wg.Add(1)
		go func() {
			defer wg.Done()

			txStart := time.Now()
			txCtx, cancel := context.WithTimeout(ctx, o.cfg.PerTransactionBudget)
			defer cancel()

			dbOrderID, userLabel, err := o.api.SubmitOrder(txCtx, row)
			res := BenchmarkResult{
				Kind:                "order_tx",
				SubmissionTS:        row.SubmissionTS.Format(time.RFC3339Nano),
				BatchKey:            batchKey,
				ParticipantID:       row.ParticipantID,
				FabricUser:          userLabel,
				AuctionSession:      row.AuctionSession,
				MarketID:            row.AuctionRef.MarketID,
				AuctionID:           row.AuctionRef.AuctionID,
				DatasetOrderID:      row.DatasetOrderID,
				ChaincodeOrderID:    dbOrderID,
				Action:              "AddOrder",
				RowsInBatch:         1,
				ExecutionDurationMS: durationMillis(time.Since(txStart)),
				Status:              "success",
			}
			if err != nil {
				res.Status = "fail"
				res.Error = err.Error()
			}

			resultCh <- res
		}()
	}

	wg.Wait()
	close(resultCh)

	failures := 0
	for result := range resultCh {
		if result.Status != "success" {
			failures++
		}
		o.results.Add(result)
	}

	batchResult := BenchmarkResult{
		Kind:                "order_batch",
		SubmissionTS:        batch[0].SubmissionTS.Truncate(time.Second).Format(time.RFC3339),
		BatchKey:            batchKey,
		AuctionSession:      batch[0].AuctionSession,
		MarketID:            batch[0].AuctionRef.MarketID,
		AuctionID:           batch[0].AuctionRef.AuctionID,
		Action:              "batch_orders",
		RowsInBatch:         len(batch),
		ExecutionDurationMS: durationMillis(time.Since(started)),
		Status:              "success",
	}
	if failures > 0 {
		batchResult.Status = "fail"
		batchResult.Error = fmt.Sprintf("%d of %d transactions failed", failures, len(batch))
	}
	o.results.Add(batchResult)

	return nil
}

func (c *APIClient) EnsureMarket(ctx context.Context, marketID string) error {
	body := map[string]string{"date": marketID}
	if err := c.postJSON(ctx, "/markets", "auctioneer", "benchmark-controller", body, nil); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return err
	}
	return nil
}

func (c *APIClient) OpenAuction(ctx context.Context, auctionID string) error {
	if err := c.postJSON(ctx, "/auctions/"+auctionID+"/open", "auctioneer", "benchmark-controller", nil, nil); err != nil {
		if strings.Contains(err.Error(), "current status: OPEN") {
			return nil
		}
		return err
	}
	return nil
}

func (c *APIClient) CloseAuction(ctx context.Context, auctionID string) error {
	if err := c.postJSON(ctx, "/auctions/"+auctionID+"/close", "auctioneer", "benchmark-controller", nil, nil); err != nil {
		if strings.Contains(err.Error(), "is already CLOSED") {
			return nil
		}
		return err
	}
	return nil
}

func (c *APIClient) SubmitOrder(ctx context.Context, row DatasetRow) (string, string, error) {
	var blocks []APIBlock
	if err := json.Unmarshal([]byte(row.CanonicalBlocksJSON), &blocks); err != nil {
		return "", "", fmt.Errorf("decode canonical blocks: %w", err)
	}

	req := addOrderRequest{
		IDAuction: row.AuctionRef.AuctionID,
		Period:    row.Period,
		Type:      row.OrderType,
		Blocks:    blocks,
	}

	var response addOrderResponse
	if err := c.postJSON(ctx, "/orders", "client", row.ParticipantID, req, &response); err != nil {
		return "", row.ParticipantID, err
	}

	return strings.TrimSpace(response.OrderID), row.ParticipantID, nil
}

func (c *APIClient) getJSON(ctx context.Context, path, role, actorID string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Actor-Role", role)
	req.Header.Set("X-Actor-ID", actorID)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return decodeAPIError(resp)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *APIClient) postJSON(ctx context.Context, path, role, actorID string, body any, out any) error {
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, payload)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Actor-Role", role)
	req.Header.Set("X-Actor-ID", actorID)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return decodeAPIError(resp)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func decodeAPIError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	var parsed apiError
	if err := json.Unmarshal(body, &parsed); err == nil && strings.TrimSpace(parsed.Error) != "" {
		return errors.New(parsed.Error)
	}
	if len(body) == 0 {
		return fmt.Errorf("api returned status %d", resp.StatusCode)
	}
	return fmt.Errorf("api returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

func loadDatasetRows(path string, loc *time.Location) ([]DatasetRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open dataset: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = false

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	indexByName := make(map[string]int, len(header))
	for i, column := range header {
		indexByName[normalizeHeader(column)] = i
	}

	required := []string{"submission_ts", "auction_session", "type", "period", "order_id", "blocks_json", "participant_id"}
	for _, name := range required {
		if _, ok := indexByName[name]; !ok {
			return nil, fmt.Errorf("dataset is missing required column %q", name)
		}
	}

	rows := make([]DatasetRow, 0, 1024)
	rowNum := 1

	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read dataset row %d: %w", rowNum+1, err)
		}
		rowNum++

		row, err := parseDatasetRow(record, indexByName, rowNum, loc)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].SubmissionTS.Equal(rows[j].SubmissionTS) {
			return rows[i].RowNumber < rows[j].RowNumber
		}
		return rows[i].SubmissionTS.Before(rows[j].SubmissionTS)
	})

	return rows, nil
}

func parseDatasetRow(record []string, indexByName map[string]int, rowNumber int, loc *time.Location) (DatasetRow, error) {
	get := func(name string) string {
		idx := indexByName[name]
		if idx < 0 || idx >= len(record) {
			return ""
		}
		return strings.TrimSpace(record[idx])
	}

	submissionRaw := get("submission_ts")
	submissionTS, err := parseFlexibleTimestamp(submissionRaw, loc)
	if err != nil {
		return DatasetRow{}, fmt.Errorf("row %d: parse submission_ts %q: %w", rowNumber, submissionRaw, err)
	}

	orderType, err := normalizeOrderType(get("type"))
	if err != nil {
		return DatasetRow{}, fmt.Errorf("row %d: %w", rowNumber, err)
	}

	period, err := strconv.Atoi(get("period"))
	if err != nil {
		return DatasetRow{}, fmt.Errorf("row %d: invalid period: %w", rowNumber, err)
	}
	if period < 1 || period > 24 {
		return DatasetRow{}, fmt.Errorf("row %d: period must be in [1,24], got %d", rowNumber, period)
	}

	auctionSession := get("auction_session")
	auctionRef, err := deriveAuctionRef(submissionTS, auctionSession)
	if err != nil {
		return DatasetRow{}, fmt.Errorf("row %d: derive auction reference: %w", rowNumber, err)
	}

	blocksJSON, blockCount, err := normalizeBlocksPayload(get("blocks_json"))
	if err != nil {
		return DatasetRow{}, fmt.Errorf("row %d: normalize blocks_json: %w", rowNumber, err)
	}

	return DatasetRow{
		RowNumber:           rowNumber,
		SubmissionRaw:       submissionRaw,
		SubmissionTS:        submissionTS,
		AuctionSession:      auctionSession,
		ParticipantID:       get("participant_id"),
		OrderType:           orderType,
		Period:              period,
		DatasetOrderID:      get("order_id"),
		CanonicalBlocksJSON: blocksJSON,
		NumBlocks:           blockCount,
		AuctionRef:          auctionRef,
	}, nil
}

func normalizeHeader(value string) string {
	value = strings.TrimSpace(strings.TrimPrefix(value, "\uFEFF"))
	value = strings.ToLower(value)
	return strings.ReplaceAll(value, " ", "_")
}

func parseFlexibleTimestamp(raw string, loc *time.Location) (time.Time, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}

	layoutsWithZone := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05Z07:00"}
	for _, layout := range layoutsWithZone {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, nil
		}
	}

	layoutsWithoutZone := []string{"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05.999999", "2006-01-02 15:04:05", "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05"}
	for _, layout := range layoutsWithoutZone {
		if ts, err := time.ParseInLocation(layout, value, loc); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported timestamp layout")
}

func normalizeOrderType(raw string) (string, error) {
	value := strings.ToUpper(strings.TrimSpace(raw))
	switch value {
	case "BUY", "B":
		return "BUY", nil
	case "SELL", "S":
		return "SELL", nil
	default:
		return "", fmt.Errorf("unsupported order type %q", raw)
	}
}

func deriveAuctionRef(submissionTS time.Time, rawSession string) (AuctionRef, error) {
	session := strings.ToUpper(strings.TrimSpace(rawSession))
	if session == "" {
		return AuctionRef{}, fmt.Errorf("empty auction_session")
	}

	if matches := fullAuctionIDPattern.FindStringSubmatch(session); matches != nil {
		marketID := matches[1]
		sessionNum, _ := strconv.Atoi(matches[2])
		return AuctionRef{MarketID: marketID, AuctionID: session, SessionNumber: sessionNum, SessionLabel: fmt.Sprintf("session_%d", sessionNum)}, nil
	}

	normalized := strings.NewReplacer("SESSION_", "", "SESSION", "", "AUCTION_", "", "AUCTION", "", "A", "", "S", "", "-", "", "_", "", " ", "").Replace(session)
	sessionNum, err := strconv.Atoi(normalized)
	if err != nil || sessionNum < 1 || sessionNum > 3 {
		return AuctionRef{}, fmt.Errorf("unsupported auction_session %q", rawSession)
	}

	marketDay := submissionTS
	if sessionNum == 1 || sessionNum == 2 {
		marketDay = marketDay.AddDate(0, 0, 1)
	}

	marketID := marketDay.Format(marketDateLayout)
	return AuctionRef{MarketID: marketID, AuctionID: fmt.Sprintf("%s-A%d", marketID, sessionNum), SessionNumber: sessionNum, SessionLabel: fmt.Sprintf("session_%d", sessionNum)}, nil
}

func normalizeBlocksPayload(raw string) (string, int, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", 0, fmt.Errorf("empty blocks_json")
	}

	var items []json.RawMessage
	if err := json.Unmarshal([]byte(value), &items); err != nil {
		return "", 0, fmt.Errorf("invalid JSON: %w", err)
	}
	if len(items) == 0 {
		return "", 0, fmt.Errorf("blocks_json is empty")
	}

	parseFloat := func(rawValue json.RawMessage) (float64, error) {
		var n float64
		if err := json.Unmarshal(rawValue, &n); err == nil {
			return n, nil
		}
		var s string
		if err := json.Unmarshal(rawValue, &s); err == nil {
			v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err != nil {
				return 0, err
			}
			return v, nil
		}
		return 0, fmt.Errorf("unsupported numeric value %s", string(rawValue))
	}

	blocks := make([]APIBlock, 0, len(items))
	for i, item := range items {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(item, &fields); err != nil {
			return "", 0, fmt.Errorf("block %d is not a JSON object: %w", i, err)
		}

		var nested map[string]json.RawMessage
		if rawBlock, ok := fields["block"]; ok && len(rawBlock) > 0 && string(rawBlock) != "null" {
			_ = json.Unmarshal(rawBlock, &nested)
		}

		findFloat := func(keys ...string) (float64, bool, error) {
			sources := []map[string]json.RawMessage{fields, nested}
			for _, src := range sources {
				if len(src) == 0 {
					continue
				}
				for _, key := range keys {
					rawValue, ok := src[key]
					if !ok || len(rawValue) == 0 || string(rawValue) == "null" {
						continue
					}
					v, err := parseFloat(rawValue)
					if err != nil {
						return 0, true, err
					}
					return v, true, nil
				}
			}
			return 0, false, nil
		}

		price, ok, err := findFloat("price_eur_mwh", "price")
		if err != nil {
			return "", 0, fmt.Errorf("block %d invalid price: %w", i, err)
		}
		if !ok {
			return "", 0, fmt.Errorf("block %d missing price", i)
		}

		amount, ok, err := findFloat("amount_mw", "amount")
		if err != nil {
			return "", 0, fmt.Errorf("block %d invalid amount: %w", i, err)
		}
		if !ok {
			return "", 0, fmt.Errorf("block %d missing amount", i)
		}
		if amount < 0 {
			return "", 0, fmt.Errorf("block %d amount must be >= 0", i)
		}

		normalizedAmount := int(math.Round(amount))
		if amount > 0 && normalizedAmount == 0 {
			normalizedAmount = 1
		}

		blocks = append(blocks, APIBlock{Amount: normalizedAmount, Price: price})
	}

	normalized, err := json.Marshal(blocks)
	if err != nil {
		return "", 0, fmt.Errorf("marshal canonical blocks JSON: %w", err)
	}

	return string(normalized), len(blocks), nil
}

func (s *ResultStore) Add(result BenchmarkResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextIdx++
	result.Index = s.nextIdx
	s.results = append(s.results, result)
}

func (s *ResultStore) Snapshot() []BenchmarkResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]BenchmarkResult, len(s.results))
	copy(out, s.results)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].SubmissionTS == out[j].SubmissionTS {
			return out[i].Index < out[j].Index
		}
		return out[i].SubmissionTS < out[j].SubmissionTS
	})
	return out
}

func writeResultsCSV(path string, results []BenchmarkResult) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create results dir: %w", err)
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create results file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"index", "kind", "submission_ts", "batch_key", "participant_id", "fabric_user", "auction_session", "market_id", "auction_id", "dataset_order_id", "chaincode_order_id", "action", "rows_in_batch", "execution_duration_ms", "status", "error"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	for _, result := range results {
		record := []string{
			strconv.Itoa(result.Index),
			result.Kind,
			result.SubmissionTS,
			result.BatchKey,
			result.ParticipantID,
			result.FabricUser,
			result.AuctionSession,
			result.MarketID,
			result.AuctionID,
			result.DatasetOrderID,
			result.ChaincodeOrderID,
			result.Action,
			strconv.Itoa(result.RowsInBatch),
			fmt.Sprintf("%.3f", result.ExecutionDurationMS),
			result.Status,
			result.Error,
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("write result row: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush results writer: %w", err)
	}
	return nil
}

func writeSummaryCSV(path string, summaries []BenchmarkSummary) error {
	if len(summaries) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create summary dir: %w", err)
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create summary file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"dataset_path", "results_path", "rows", "results", "succeeded", "failed", "duration_ms", "status", "error"}); err != nil {
		return err
	}

	for _, item := range summaries {
		record := []string{
			item.DatasetPath,
			item.ResultsPath,
			strconv.Itoa(item.Rows),
			strconv.Itoa(item.Results),
			strconv.Itoa(item.Succeeded),
			strconv.Itoa(item.Failed),
			fmt.Sprintf("%.3f", item.DurationMS),
			item.Status,
			item.ErrorMessage,
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	writer.Flush()
	return writer.Error()
}

func durationMillis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

func parseDurationEnv(name string, defaultValue time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return defaultValue, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid duration in %s: %w", name, err)
	}
	return d, nil
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func envOrDefault(name, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return defaultValue
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
