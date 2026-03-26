package main

import (
	"context"
	"crypto"
	"crypto/sha256"
	"crypto/x509"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
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

	"github.com/hyperledger/fabric-gateway/pkg/client"
	gatewayhash "github.com/hyperledger/fabric-gateway/pkg/hash"
	"github.com/hyperledger/fabric-gateway/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const marketDateLayout = "02-01-2006"

var (
	fullAuctionIDPattern = regexp.MustCompile(`^(\d{2}-\d{2}-\d{4})-A([123])$`)
	userPathPattern      = regexp.MustCompile(`User\d+@`)
)

type GatewayConfig struct {
	ChannelName         string
	ChaincodeName       string
	MSPID               string
	PeerEndpoint        string
	PeerTLSHostOverride string
	ClientCertPath      string
	ClientKeyPath       string
	PeerTLSRootCertPath string
	EvaluateTimeout     time.Duration
	EndorseTimeout      time.Duration
	SubmitTimeout       time.Duration
	CommitStatusTimeout time.Duration
}

type BenchmarkConfig struct {
	DatasetPath          string
	ResultsPath          string
	Timezone             string
	LogLevel             slog.Level
	Controller           GatewayConfig
	ParticipantTemplate  GatewayConfig
	PerTransactionBudget time.Duration
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

type ResultStore struct {
	mu      sync.Mutex
	nextIdx int
	results []BenchmarkResult
}

type ControllerClient struct {
	cfg      GatewayConfig
	gateway  *client.Gateway
	grpcConn *grpc.ClientConn
	contract *client.Contract
}

type ParticipantIdentity struct {
	ParticipantID string
	UserLabel     string
	Config        GatewayConfig
	Certificate   *x509.Certificate
	PrivateKey    crypto.PrivateKey
}

type ParticipantClientPool struct {
	cfg      GatewayConfig
	grpcConn *grpc.ClientConn

	mu       sync.Mutex
	sessions map[string]*ParticipantIdentity
}

type Orchestrator struct {
	cfg          BenchmarkConfig
	loc          *time.Location
	controllerCh chan<- ControllerCommand
	participants *ParticipantClientPool
	results      *ResultStore
	logger       *slog.Logger
}

type ChaincodeBlock struct {
	Amount int     `json:"amount"`
	Price  float64 `json:"price"`
	Status string  `json:"status,omitempty"`
}

type rawDatasetBlock struct {
	Block       *rawDatasetBlock `json:"block"`
	PriceEURMWh *float64         `json:"price_eur_mwh"`
	AmountMW    *float64         `json:"amount_mw"`
	Price       *float64         `json:"price"`
	Amount      *float64         `json:"amount"`
}

type DigestBlock struct {
	Amount float64 `json:"amount"`
	Price  float64 `json:"price"`
	Status string  `json:"status,omitempty"`
}

type OrderDigestPayload struct {
	IDAuction string        `json:"id_auction"`
	Period    int           `json:"period"`
	Type      string        `json:"type"`
	Blocks    []DigestBlock `json:"blocks"`
}

func main() {
	cfg, err := loadBenchmarkConfig()
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

	rows, err := loadDatasetRows(cfg.DatasetPath, loc)
	if err != nil {
		logger.Error("failed to load dataset", "path", cfg.DatasetPath, "error", err)
		os.Exit(1)
	}
	if len(rows) == 0 {
		logger.Error("dataset is empty", "path", cfg.DatasetPath)
		os.Exit(1)
	}

	controllerClient, err := newControllerClient(cfg.Controller)
	if err != nil {
		logger.Error("failed to initialize controller client", "error", err)
		os.Exit(1)
	}
	defer controllerClient.Close()

	participantPool, err := newParticipantClientPool(cfg.ParticipantTemplate)
	if err != nil {
		logger.Error("failed to initialize participant pool", "error", err)
		os.Exit(1)
	}
	defer participantPool.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	results := &ResultStore{}
	controllerCh := make(chan ControllerCommand)
	controllerDone := make(chan error, 1)

	go func() {
		controllerDone <- runController(ctx, controllerClient, controllerCh)
	}()

	orchestrator := &Orchestrator{
		cfg:          cfg,
		loc:          loc,
		controllerCh: controllerCh,
		participants: participantPool,
		results:      results,
		logger:       logger,
	}

	logger.Info("benchmark replay started",
		"dataset", cfg.DatasetPath,
		"rows", len(rows),
		"timezone", cfg.Timezone,
		"results", cfg.ResultsPath,
	)

	runErr := orchestrator.Run(ctx, rows)
	close(controllerCh)

	controllerErr := <-controllerDone
	if runErr == nil && controllerErr != nil && !errors.Is(controllerErr, context.Canceled) {
		runErr = controllerErr
	}

	writeErr := writeResultsCSV(cfg.ResultsPath, results.Snapshot())
	if writeErr != nil {
		logger.Error("failed to write benchmark results", "path", cfg.ResultsPath, "error", writeErr)
		if runErr == nil {
			runErr = writeErr
		}
	}

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		logger.Error("benchmark replay failed", "error", runErr)
		os.Exit(1)
	}

	logger.Info("benchmark replay finished",
		"rows", len(rows),
		"results", len(results.Snapshot()),
		"results_path", cfg.ResultsPath,
	)
}

func loadBenchmarkConfig() (BenchmarkConfig, error) {
	datasetPath := flag.String("dataset", "", "Path to the replay dataset CSV")
	resultsPath := flag.String("results", "benchmark_results.csv", "Output CSV path")
	flag.Parse()

	if strings.TrimSpace(*datasetPath) == "" {
		return BenchmarkConfig{}, fmt.Errorf("--dataset is required")
	}

	channelName := envOrDefault("BENCH_CHANNEL_NAME", "mychannel")
	chaincodeName := envOrDefault("BENCH_CHAINCODE_NAME", "market")

	evaluateTimeout, err := parseDurationEnv("BENCH_EVALUATE_TIMEOUT", 10*time.Second)
	if err != nil {
		return BenchmarkConfig{}, err
	}
	endorseTimeout, err := parseDurationEnv("BENCH_ENDORSE_TIMEOUT", 15*time.Second)
	if err != nil {
		return BenchmarkConfig{}, err
	}
	submitTimeout, err := parseDurationEnv("BENCH_SUBMIT_TIMEOUT", 15*time.Second)
	if err != nil {
		return BenchmarkConfig{}, err
	}
	commitStatusTimeout, err := parseDurationEnv("BENCH_COMMIT_STATUS_TIMEOUT", time.Minute)
	if err != nil {
		return BenchmarkConfig{}, err
	}

	controllerCfg := GatewayConfig{
		ChannelName:         channelName,
		ChaincodeName:       chaincodeName,
		MSPID:               envOrDefault("BENCH_CONTROLLER_MSP_ID", "Org1MSP"),
		PeerEndpoint:        strings.TrimSpace(envOrDefault("BENCH_CONTROLLER_PEER_ENDPOINT", "dns:///localhost:7051")),
		PeerTLSHostOverride: strings.TrimSpace(envOrDefault("BENCH_CONTROLLER_PEER_TLS_HOST_OVERRIDE", "peer0.org1.example.com")),
		ClientCertPath:      strings.TrimSpace(envOrDefault("BENCH_CONTROLLER_CLIENT_CERT_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/signcerts")),
		ClientKeyPath:       strings.TrimSpace(envOrDefault("BENCH_CONTROLLER_CLIENT_KEY_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/keystore")),
		PeerTLSRootCertPath: strings.TrimSpace(envOrDefault("BENCH_CONTROLLER_PEER_TLS_ROOTCERT_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt")),
		EvaluateTimeout:     evaluateTimeout,
		EndorseTimeout:      endorseTimeout,
		SubmitTimeout:       submitTimeout,
		CommitStatusTimeout: commitStatusTimeout,
	}

	participantCfg := GatewayConfig{
		ChannelName:         channelName,
		ChaincodeName:       chaincodeName,
		MSPID:               envOrDefault("BENCH_PARTICIPANT_MSP_ID", "Org2MSP"),
		PeerEndpoint:        strings.TrimSpace(envOrDefault("BENCH_PARTICIPANT_PEER_ENDPOINT", "dns:///localhost:9051")),
		PeerTLSHostOverride: strings.TrimSpace(envOrDefault("BENCH_PARTICIPANT_PEER_TLS_HOST_OVERRIDE", "peer0.org2.example.com")),
		ClientCertPath:      strings.TrimSpace(envOrDefault("BENCH_PARTICIPANT_CLIENT_CERT_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org2.example.com/users/User1@org2.example.com/msp/signcerts")),
		ClientKeyPath:       strings.TrimSpace(envOrDefault("BENCH_PARTICIPANT_CLIENT_KEY_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org2.example.com/users/User1@org2.example.com/msp/keystore")),
		PeerTLSRootCertPath: strings.TrimSpace(envOrDefault("BENCH_PARTICIPANT_PEER_TLS_ROOTCERT_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt")),
		EvaluateTimeout:     evaluateTimeout,
		EndorseTimeout:      endorseTimeout,
		SubmitTimeout:       submitTimeout,
		CommitStatusTimeout: commitStatusTimeout,
	}

	perTxBudget := submitTimeout + commitStatusTimeout + 5*time.Second

	cfg := BenchmarkConfig{
		DatasetPath:          filepath.Clean(*datasetPath),
		ResultsPath:          filepath.Clean(*resultsPath),
		Timezone:             envOrDefault("APP_TIMEZONE", "Europe/Madrid"),
		LogLevel:             parseLogLevel(envOrDefault("APP_LOG_LEVEL", "INFO")),
		Controller:           controllerCfg,
		ParticipantTemplate:  participantCfg,
		PerTransactionBudget: perTxBudget,
	}

	if err := validateGatewayConfig("controller", cfg.Controller); err != nil {
		return BenchmarkConfig{}, err
	}
	if err := validateGatewayConfig("participant", cfg.ParticipantTemplate); err != nil {
		return BenchmarkConfig{}, err
	}

	return cfg, nil
}

func validateGatewayConfig(name string, cfg GatewayConfig) error {
	missing := make([]string, 0, 4)
	if strings.TrimSpace(cfg.PeerEndpoint) == "" {
		missing = append(missing, "PeerEndpoint")
	}
	if strings.TrimSpace(cfg.ClientCertPath) == "" {
		missing = append(missing, "ClientCertPath")
	}
	if strings.TrimSpace(cfg.ClientKeyPath) == "" {
		missing = append(missing, "ClientKeyPath")
	}
	if strings.TrimSpace(cfg.PeerTLSRootCertPath) == "" {
		missing = append(missing, "PeerTLSRootCertPath")
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s config is missing required fields: %s", name, strings.Join(missing, ", "))
	}
	return nil
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

	layoutsWithZone := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05Z07:00",
	}
	for _, layout := range layoutsWithZone {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, nil
		}
	}

	layoutsWithoutZone := []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
	}
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
		return AuctionRef{
			MarketID:      marketID,
			AuctionID:     session,
			SessionNumber: sessionNum,
			SessionLabel:  fmt.Sprintf("session_%d", sessionNum),
		}, nil
	}

	normalized := strings.NewReplacer(
		"SESSION_", "",
		"SESSION", "",
		"AUCTION_", "",
		"AUCTION", "",
		"A", "",
		"S", "",
		"-", "",
		"_", "",
		" ", "",
	).Replace(session)

	sessionNum, err := strconv.Atoi(normalized)
	if err != nil || sessionNum < 1 || sessionNum > 3 {
		return AuctionRef{}, fmt.Errorf("unsupported auction_session %q", rawSession)
	}

	marketDay := submissionTS
	if sessionNum == 1 || sessionNum == 2 {
		marketDay = marketDay.AddDate(0, 0, 1)
	}

	marketID := marketDay.Format(marketDateLayout)
	return AuctionRef{
		MarketID:      marketID,
		AuctionID:     fmt.Sprintf("%s-A%d", marketID, sessionNum),
		SessionNumber: sessionNum,
		SessionLabel:  fmt.Sprintf("session_%d", sessionNum),
	}, nil
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

	parseFloat := func(raw json.RawMessage) (float64, error) {
		var n float64
		if err := json.Unmarshal(raw, &n); err == nil {
			return n, nil
		}

		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err != nil {
				return 0, err
			}
			return n, nil
		}

		return 0, fmt.Errorf("unsupported numeric value %s", string(raw))
	}

	blocks := make([]ChaincodeBlock, 0, len(items))

	for i, item := range items {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(item, &fields); err != nil {
			return "", 0, fmt.Errorf("block %d is not a JSON object: %w", i, err)
		}

		var nested map[string]json.RawMessage
		if rawBlock, ok := fields["block"]; ok && len(rawBlock) > 0 && string(rawBlock) != "null" {
			// Accept both:
			// 1) {"block": 1, "price_eur_mwh": ..., "amount_mw": ...}
			// 2) {"block": {"price_eur_mwh": ..., "amount_mw": ...}}
			_ = json.Unmarshal(rawBlock, &nested) // ignore error when "block" is just a number
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

		if amount <= 0 {
			return "", 0, fmt.Errorf("block %d amount must be > 0", i)
		}

		blocks = append(blocks, ChaincodeBlock{
			Amount: int(math.Round(amount)),
			Price:  price,
			Status: "USED",
		})
	}

	normalized, err := json.Marshal(blocks)
	if err != nil {
		return "", 0, fmt.Errorf("marshal canonical blocks JSON: %w", err)
	}

	return string(normalized), len(blocks), nil
}

func computeOrderDigest(idAuction string, period int, orderType string, blocksJSON string) (string, error) {
	var blocks []DigestBlock
	if err := json.Unmarshal([]byte(blocksJSON), &blocks); err != nil {
		return "", fmt.Errorf("decode canonical blocks JSON: %w", err)
	}

	// Hash is defined only over amount/price; ignore status if present.
	for i := range blocks {
		blocks[i].Status = ""
	}

	payload := OrderDigestPayload{
		IDAuction: idAuction,
		Period:    period,
		Type:      orderType,
		Blocks:    blocks,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal order digest payload: %w", err)
	}

	return sha256Hex(b), nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func firstFloat(values ...*float64) (float64, bool) {
	for _, value := range values {
		if value != nil {
			return *value, true
		}
	}
	return 0, false
}

func newControllerClient(cfg GatewayConfig) (*ControllerClient, error) {
	grpcConn, err := newGRPCConnection(cfg)
	if err != nil {
		return nil, err
	}

	id, err := newIdentity(cfg)
	if err != nil {
		_ = grpcConn.Close()
		return nil, err
	}

	sign, err := newSign(cfg)
	if err != nil {
		_ = grpcConn.Close()
		return nil, err
	}

	gateway, err := client.Connect(
		id,
		client.WithSign(sign),
		client.WithHash(gatewayhash.SHA256),
		client.WithClientConnection(grpcConn),
		client.WithEvaluateTimeout(cfg.EvaluateTimeout),
		client.WithEndorseTimeout(cfg.EndorseTimeout),
		client.WithSubmitTimeout(cfg.SubmitTimeout),
		client.WithCommitStatusTimeout(cfg.CommitStatusTimeout),
	)
	if err != nil {
		_ = grpcConn.Close()
		return nil, fmt.Errorf("connect gateway: %w", err)
	}

	network := gateway.GetNetwork(cfg.ChannelName)
	contract := network.GetContract(cfg.ChaincodeName)

	return &ControllerClient{
		cfg:      cfg,
		gateway:  gateway,
		grpcConn: grpcConn,
		contract: contract,
	}, nil
}

func (c *ControllerClient) Close() {
	if c.gateway != nil {
		c.gateway.Close()
	}
	if c.grpcConn != nil {
		_ = c.grpcConn.Close()
	}
}

func (c *ControllerClient) EnsureMarket(ctx context.Context, marketID string) error {
	_, err := c.contract.SubmitWithContext(ctx, "CreateMarket", client.WithArguments(marketID))
	if err != nil {
		err = wrapGatewayError("CreateMarket", err)
		if isMarketAlreadyExistsError(err) {
			return nil
		}
		return err
	}
	return nil
}

func (c *ControllerClient) OpenAuction(ctx context.Context, auctionID string) error {
	_, err := c.contract.SubmitWithContext(ctx, "OpenAuction", client.WithArguments(auctionID))
	if err != nil {
		err = wrapGatewayError("OpenAuction", err)
		if isAuctionAlreadyOpenError(err) {
			return nil
		}
		return err
	}
	return nil
}

func (c *ControllerClient) CloseAuction(ctx context.Context, auctionID string) error {
	_, err := c.contract.SubmitWithContext(ctx, "CloseAuction", client.WithArguments(auctionID))
	if err != nil {
		err = wrapGatewayError("CloseAuction", err)
		if isAuctionAlreadyClosedError(err) {
			return nil
		}
		return err
	}
	return nil
}

func newParticipantClientPool(cfg GatewayConfig) (*ParticipantClientPool, error) {
	conn, err := newGRPCConnection(cfg)
	if err != nil {
		return nil, err
	}

	return &ParticipantClientPool{
		cfg:      cfg,
		grpcConn: conn,
		sessions: make(map[string]*ParticipantIdentity),
	}, nil
}

func (p *ParticipantClientPool) Close() {
	if p.grpcConn != nil {
		_ = p.grpcConn.Close()
	}
}

func (p *ParticipantClientPool) getOrCreateIdentity(participantID string) (*ParticipantIdentity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if session, ok := p.sessions[participantID]; ok {
		return session, nil
	}

	userLabel := fmt.Sprintf("User%d", len(p.sessions)+1)
	userCfg, err := deriveUserGatewayConfig(p.cfg, userLabel)
	if err != nil {
		return nil, err
	}

	certPEM, err := readPEM(userCfg.ClientCertPath)
	if err != nil {
		return nil, fmt.Errorf("load certificate for participant %s (%s): %w", participantID, userLabel, err)
	}
	certificate, err := identity.CertificateFromPEM(certPEM)
	if err != nil {
		return nil, fmt.Errorf("parse certificate for participant %s (%s): %w", participantID, userLabel, err)
	}

	keyPEM, err := readPEM(userCfg.ClientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load private key for participant %s (%s): %w", participantID, userLabel, err)
	}
	privateKey, err := identity.PrivateKeyFromPEM(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("parse private key for participant %s (%s): %w", participantID, userLabel, err)
	}

	session := &ParticipantIdentity{
		ParticipantID: participantID,
		UserLabel:     userLabel,
		Config:        userCfg,
		Certificate:   certificate,
		PrivateKey:    privateKey,
	}
	p.sessions[participantID] = session
	return session, nil
}

func deriveUserGatewayConfig(template GatewayConfig, userLabel string) (GatewayConfig, error) {
	replace := func(path string) (string, error) {
		if !userPathPattern.MatchString(path) {
			return "", fmt.Errorf("path %q does not contain a UserN@ segment", path)
		}
		return userPathPattern.ReplaceAllString(path, userLabel+"@"), nil
	}

	certPath, err := replace(template.ClientCertPath)
	if err != nil {
		return GatewayConfig{}, fmt.Errorf("derive certificate path: %w", err)
	}
	keyPath, err := replace(template.ClientKeyPath)
	if err != nil {
		return GatewayConfig{}, fmt.Errorf("derive key path: %w", err)
	}

	derived := template
	derived.ClientCertPath = certPath
	derived.ClientKeyPath = keyPath
	return derived, nil
}

func (p *ParticipantClientPool) SubmitOrder(ctx context.Context, row DatasetRow) (string, string, error) {
	session, err := p.getOrCreateIdentity(row.ParticipantID)
	if err != nil {
		return "", "", err
	}

	id, err := identity.NewX509Identity(session.Config.MSPID, session.Certificate)
	if err != nil {
		return "", session.UserLabel, fmt.Errorf("create X509 identity: %w", err)
	}
	sign, err := identity.NewPrivateKeySign(session.PrivateKey)
	if err != nil {
		return "", session.UserLabel, fmt.Errorf("create signer: %w", err)
	}

	gateway, err := client.Connect(
		id,
		client.WithSign(sign),
		client.WithHash(gatewayhash.SHA256),
		client.WithClientConnection(p.grpcConn),
		client.WithEvaluateTimeout(session.Config.EvaluateTimeout),
		client.WithEndorseTimeout(session.Config.EndorseTimeout),
		client.WithSubmitTimeout(session.Config.SubmitTimeout),
		client.WithCommitStatusTimeout(session.Config.CommitStatusTimeout),
	)
	if err != nil {
		return "", session.UserLabel, fmt.Errorf("connect participant gateway: %w", err)
	}
	defer gateway.Close()

	network := gateway.GetNetwork(session.Config.ChannelName)
	contract := network.GetContract(session.Config.ChaincodeName)

	orderHash, err := computeOrderDigest(row.AuctionRef.AuctionID, row.Period, row.OrderType, row.CanonicalBlocksJSON)
	if err != nil {
		return "", session.UserLabel, fmt.Errorf("compute order hash: %w", err)
	}

	result, err := contract.SubmitWithContext(
		ctx,
		"AddOrder",
		client.WithArguments(row.AuctionRef.AuctionID, strconv.Itoa(row.Period), row.OrderType, "", orderHash),
		client.WithTransient(map[string][]byte{
			"blocks": []byte(row.CanonicalBlocksJSON),
		}),
	)
	if err != nil {
		return "", session.UserLabel, wrapGatewayError("AddOrder", err)
	}

	var chaincodeOrderID string
	if err := json.Unmarshal(result, &chaincodeOrderID); err == nil {
		return chaincodeOrderID, session.UserLabel, nil
	}

	return strings.TrimSpace(string(result)), session.UserLabel, nil
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

	cmd := ControllerCommand{
		Action:       action,
		MarketID:     marketID,
		AuctionID:    auctionID,
		SubmissionTS: submissionTS,
		Reply:        reply,
	}

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

			chaincodeOrderID, fabricUser, err := o.participants.SubmitOrder(txCtx, row)
			res := BenchmarkResult{
				Kind:                "order_tx",
				SubmissionTS:        row.SubmissionTS.Format(time.RFC3339Nano),
				BatchKey:            batchKey,
				ParticipantID:       row.ParticipantID,
				FabricUser:          fabricUser,
				AuctionSession:      row.AuctionSession,
				MarketID:            row.AuctionRef.MarketID,
				AuctionID:           row.AuctionRef.AuctionID,
				DatasetOrderID:      row.DatasetOrderID,
				ChaincodeOrderID:    chaincodeOrderID,
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

func runController(ctx context.Context, controller *ControllerClient, commands <-chan ControllerCommand) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case cmd, ok := <-commands:
			if !ok {
				return nil
			}

			started := time.Now()
			actionCtx, cancel := context.WithTimeout(ctx, controller.cfg.SubmitTimeout+controller.cfg.CommitStatusTimeout+5*time.Second)

			var err error
			switch cmd.Action {
			case ActionOpenMarket:
				err = controller.EnsureMarket(actionCtx, cmd.MarketID)
			case ActionOpenAuction:
				err = controller.OpenAuction(actionCtx, cmd.AuctionID)
			case ActionCloseAuction:
				fmt.Println("Close Auction YES!")
				err = controller.CloseAuction(actionCtx, cmd.AuctionID)
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
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create results file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{
		"index",
		"kind",
		"submission_ts",
		"batch_key",
		"participant_id",
		"fabric_user",
		"auction_session",
		"market_id",
		"auction_id",
		"dataset_order_id",
		"chaincode_order_id",
		"action",
		"rows_in_batch",
		"execution_duration_ms",
		"status",
		"error",
	}
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

func durationMillis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

func newGRPCConnection(cfg GatewayConfig) (*grpc.ClientConn, error) {
	tlsPEM, err := readPEM(cfg.PeerTLSRootCertPath)
	if err != nil {
		return nil, fmt.Errorf("read peer TLS root cert: %w", err)
	}

	tlsCert, err := identity.CertificateFromPEM(tlsPEM)
	if err != nil {
		return nil, fmt.Errorf("parse peer TLS root cert: %w", err)
	}

	certPool := x509.NewCertPool()
	certPool.AddCert(tlsCert)
	transportCredentials := credentials.NewClientTLSFromCert(certPool, cfg.PeerTLSHostOverride)

	target := cfg.PeerEndpoint
	if !strings.Contains(target, ":///") {
		target = "dns:///" + target
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		return nil, fmt.Errorf("create gRPC client for %s: %w", cfg.PeerEndpoint, err)
	}

	return conn, nil
}

func newIdentity(cfg GatewayConfig) (*identity.X509Identity, error) {
	certPEM, err := readPEM(cfg.ClientCertPath)
	if err != nil {
		return nil, fmt.Errorf("read client certificate: %w", err)
	}

	certificate, err := identity.CertificateFromPEM(certPEM)
	if err != nil {
		return nil, fmt.Errorf("parse client certificate: %w", err)
	}

	id, err := identity.NewX509Identity(cfg.MSPID, certificate)
	if err != nil {
		return nil, fmt.Errorf("create X509 identity: %w", err)
	}

	return id, nil
}

func newSign(cfg GatewayConfig) (identity.Sign, error) {
	keyPEM, err := readPEM(cfg.ClientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read client private key: %w", err)
	}

	privateKey, err := identity.PrivateKeyFromPEM(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("parse client private key: %w", err)
	}

	sign, err := identity.NewPrivateKeySign(privateKey)
	if err != nil {
		return nil, fmt.Errorf("create signer: %w", err)
	}

	return sign, nil
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

func readPEM(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return os.ReadFile(path)
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		return os.ReadFile(filepath.Join(path, entry.Name()))
	}

	return nil, fmt.Errorf("no file found in directory %s", path)
}

func wrapGatewayError(txName string, err error) error {
	var transactionErr *client.TransactionError
	if errors.As(err, &transactionErr) && len(transactionErr.Details()) > 0 {
		var b strings.Builder
		b.WriteString(txName)
		b.WriteString(" failed: ")
		b.WriteString(transactionErr.Error())
		for _, detail := range transactionErr.Details() {
			if detail == nil {
				continue
			}
			b.WriteString(" | peer=")
			b.WriteString(detail.GetAddress())
			b.WriteString(" msp=")
			b.WriteString(detail.GetMspId())
			b.WriteString(" msg=")
			b.WriteString(detail.GetMessage())
		}
		return errors.New(b.String())
	}

	return fmt.Errorf("%s failed: %w", txName, err)
}

func isAuctionAlreadyOpenError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "must be NEWLY_CREATED to open") && strings.Contains(msg, "current status: OPEN")
}

func isAuctionAlreadyClosedError(err error) bool {
	return strings.Contains(err.Error(), "is already CLOSED")
}

func isMarketAlreadyExistsError(err error) bool {
	return strings.Contains(err.Error(), "already exists")
}
