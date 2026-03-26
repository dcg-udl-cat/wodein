package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	APIBaseURL         string
	RequestTimeout     time.Duration
	CheckpointFile     string
	PollWait           time.Duration
	ReconnectDelay     time.Duration
	MaxParallelPeriods int
	LogLevel           slog.Level
}

type CloseAuctionEvent struct {
	IDAuction string `json:"id_auction"`
}

type Block struct {
	Amount int     `json:"amount"`
	Price  float64 `json:"price"`
}

type Order struct {
	IDOrder   string  `json:"id_order"`
	IDAuction string  `json:"id_auction"`
	Period    int     `json:"period"`
	Type      string  `json:"type"`
	Blocks    []Block `json:"blocks"`
	ClientID  string  `json:"client_id"`
}

type Settlement struct {
	Orders map[string][]Order `json:"orders"`
}

type AuctionContext struct {
	AuctionID string
	MarketID  string
	Periods   []int
}

type ClearingRequest struct {
	Auction AuctionContext
	Orders  []Order
}

type ClearingAlgorithm interface {
	Name() string
	Clear(ctx context.Context, req ClearingRequest) (Settlement, error)
}

type AuctionContextResolver interface {
	Resolve(auctionID string) (AuctionContext, error)
}

type LedgerGateway interface {
	GetOrdersByAuction(ctx context.Context, auctionID string) ([]Order, error)
	UpdateSettlement(ctx context.Context, marketID string, settlement Settlement) error
}

type APILedger struct {
	client *APIClient
}

type SettlementService struct {
	ledger   LedgerGateway
	resolver AuctionContextResolver
	clearers map[string]ClearingAlgorithm
	current  string
	logger   *slog.Logger
}

type SettlementListener struct {
	cfg        Config
	logger     *slog.Logger
	client     *APIClient
	checkpoint *FileCheckpoint
	processor  *SettlementService
}

type AuctionIDResolver struct{}

type GreedyOrderBookClearing struct {
	MaxParallelPeriods int
}

type pricedBlock struct {
	Order     Order
	BlockIdx  int
	Price     float64
	Remaining int
}

type periodResult struct {
	Period int
	Orders []Order
	Err    error
}

type APIClient struct {
	baseURL string
	http    *http.Client
}

type apiEvent struct {
	ID        int64           `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt string          `json:"created_at"`
}

type apiError struct {
	Error string `json:"error"`
}

type FileCheckpoint struct {
	path string
	mu   sync.Mutex
	last int64
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	checkpoint, err := NewFileCheckpoint(cfg.CheckpointFile)
	if err != nil {
		logger.Error("failed to initialize checkpoint", "path", cfg.CheckpointFile, "error", err)
		os.Exit(1)
	}

	apiClient := &APIClient{
		baseURL: strings.TrimRight(cfg.APIBaseURL, "/"),
		http:    &http.Client{Timeout: cfg.RequestTimeout},
	}

	ledger := &APILedger{client: apiClient}

	settlementService := &SettlementService{
		ledger:   ledger,
		resolver: AuctionIDResolver{},
		clearers: map[string]ClearingAlgorithm{},
		logger:   logger,
	}

	settlementService.RegisterClearingAlgorithm(&GreedyOrderBookClearing{
		MaxParallelPeriods: cfg.MaxParallelPeriods,
	})
	settlementService.UseClearingAlgorithm("greedy-order-book")

	listener := &SettlementListener{
		cfg:        cfg,
		logger:     logger,
		client:     apiClient,
		checkpoint: checkpoint,
		processor:  settlementService,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info(
		"http settlement listener started",
		"api", cfg.APIBaseURL,
		"checkpoint_file", cfg.CheckpointFile,
		"clearing_algorithm", settlementService.current,
	)

	if err := listener.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("settlement listener stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("http settlement listener stopped")
}

func loadConfig() (Config, error) {
	cfg := Config{
		APIBaseURL:     envOrDefault("APP_API_BASE_URL", "http://127.0.0.1:8080"),
		CheckpointFile: envOrDefault("APP_CHECKPOINT_FILE", "./close-auction.checkpoint"),
		LogLevel:       parseLogLevel(envOrDefault("APP_LOG_LEVEL", "INFO")),
	}

	var err error
	cfg.RequestTimeout, err = parseDurationEnv("APP_REQUEST_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.PollWait, err = parseDurationEnv("APP_EVENT_WAIT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.ReconnectDelay, err = parseDurationEnv("APP_RECONNECT_DELAY", 2*time.Second)
	if err != nil {
		return Config{}, err
	}

	cfg.MaxParallelPeriods = 4
	if raw := strings.TrimSpace(os.Getenv("APP_MAX_PARALLEL_PERIODS")); raw != "" {
		var n int
		if _, err := fmt.Sscanf(raw, "%d", &n); err != nil || n <= 0 {
			return Config{}, fmt.Errorf("invalid APP_MAX_PARALLEL_PERIODS: %q", raw)
		}
		cfg.MaxParallelPeriods = n
	}

	if strings.TrimSpace(cfg.APIBaseURL) == "" {
		return Config{}, fmt.Errorf("APP_API_BASE_URL is required")
	}

	return cfg, nil
}

func (l *SettlementListener) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		afterID := l.checkpoint.Last()
		events, err := l.client.GetEvents(ctx, afterID, "CloseAuction", l.cfg.PollWait)
		if err != nil {
			l.logger.Error("failed to poll events", "after_id", afterID, "error", err)
			if err := sleepWithContext(ctx, l.cfg.ReconnectDelay); err != nil {
				return err
			}
			continue
		}

		if len(events) == 0 {
			continue
		}

		for _, event := range events {
			if event.Type != "CloseAuction" {
				if err := l.checkpoint.Save(event.ID); err != nil {
					return err
				}
				continue
			}

			if err := l.handleCloseAuctionEvent(ctx, event); err != nil {
				l.logger.Error(
					"failed to process CloseAuction event",
					"event_id", event.ID,
					"error", err,
				)
				if err := sleepWithContext(ctx, l.cfg.ReconnectDelay); err != nil {
					return err
				}
				break
			}

			if err := l.checkpoint.Save(event.ID); err != nil {
				return err
			}
		}
	}
}

func (l *SettlementListener) handleCloseAuctionEvent(ctx context.Context, event apiEvent) error {
	var payload CloseAuctionEvent
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode CloseAuction payload: %w", err)
	}
	if strings.TrimSpace(payload.IDAuction) == "" {
		return fmt.Errorf("CloseAuction payload is missing id_auction")
	}

	l.logger.Info(
		"CloseAuction event received",
		"auction_id", payload.IDAuction,
		"event_id", event.ID,
	)

	if err := l.processor.ProcessAuctionClosed(ctx, payload.IDAuction); err != nil {
		return err
	}

	l.logger.Info(
		"settlement published",
		"auction_id", payload.IDAuction,
		"event_id", event.ID,
	)

	return nil
}

func (s *SettlementService) RegisterClearingAlgorithm(algorithm ClearingAlgorithm) {
	if algorithm == nil {
		panic("clearing algorithm is nil")
	}
	if s.clearers == nil {
		s.clearers = map[string]ClearingAlgorithm{}
	}
	s.clearers[algorithm.Name()] = algorithm
}

func (s *SettlementService) UseClearingAlgorithm(name string) {
	if _, ok := s.clearers[name]; !ok {
		panic("unknown clearing algorithm: " + name)
	}
	s.current = name
}

func (s *SettlementService) ProcessAuctionClosed(ctx context.Context, auctionID string) error {
	resolverResult, err := s.resolver.Resolve(auctionID)
	if err != nil {
		return fmt.Errorf("resolve auction context: %w", err)
	}

	orders, err := s.ledger.GetOrdersByAuction(ctx, auctionID)
	if err != nil {
		return fmt.Errorf("GetOrdersByAuction failed: %w", err)
	}

	clearer, ok := s.clearers[s.current]
	if !ok {
		return fmt.Errorf("no clearing algorithm selected")
	}

	settlement, err := clearer.Clear(ctx, ClearingRequest{
		Auction: resolverResult,
		Orders:  orders,
	})
	if err != nil {
		return fmt.Errorf("clear auction %s using %s: %w", auctionID, clearer.Name(), err)
	}

	if err := s.ledger.UpdateSettlement(ctx, resolverResult.MarketID, settlement); err != nil {
		return fmt.Errorf("UpdateSettlement failed: %w", err)
	}

	s.logger.Info(
		"auction processed",
		"auction_id", resolverResult.AuctionID,
		"market_id", resolverResult.MarketID,
		"orders", len(orders),
		"algorithm", clearer.Name(),
	)

	return nil
}

func (l *APILedger) GetOrdersByAuction(ctx context.Context, auctionID string) ([]Order, error) {
	var orders []Order
	if err := l.client.getJSON(ctx, "/auctions/"+auctionID+"/orders", "auctioneer", "oracle", &orders); err != nil {
		return nil, err
	}
	if orders == nil {
		return []Order{}, nil
	}
	return orders, nil
}

func (l *APILedger) UpdateSettlement(ctx context.Context, marketID string, settlement Settlement) error {
	return l.client.postJSON(ctx, "/settlements/"+marketID, "auctioneer", "oracle", settlement, nil)
}

func (AuctionIDResolver) Resolve(auctionID string) (AuctionContext, error) {
	auctionID = strings.TrimSpace(auctionID)
	if auctionID == "" {
		return AuctionContext{}, fmt.Errorf("auction ID is required")
	}

	idx := strings.LastIndex(auctionID, "-")
	if idx <= 0 || idx == len(auctionID)-1 {
		return AuctionContext{}, fmt.Errorf("invalid auction ID format: %s", auctionID)
	}

	marketID := auctionID[:idx]
	suffix := strings.ToUpper(strings.TrimSpace(auctionID[idx+1:]))

	var periods []int
	switch suffix {
	case "A1", "A2":
		periods = makeRange(1, 24)
	case "A3":
		periods = makeRange(13, 24)
	default:
		return AuctionContext{}, fmt.Errorf("unsupported auction suffix %q in auction ID %s", suffix, auctionID)
	}

	return AuctionContext{
		AuctionID: auctionID,
		MarketID:  marketID,
		Periods:   periods,
	}, nil
}

func (g *GreedyOrderBookClearing) Name() string {
	return "greedy-order-book"
}

func (g *GreedyOrderBookClearing) Clear(ctx context.Context, req ClearingRequest) (Settlement, error) {
	periods := collectPeriods(req.Auction.Periods, req.Orders)
	settlement := Settlement{Orders: make(map[string][]Order, len(periods))}

	if len(periods) == 0 {
		return settlement, nil
	}

	workerCount := g.MaxParallelPeriods
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(periods) {
		workerCount = len(periods)
	}

	jobs := make(chan int)
	results := make(chan periodResult, len(periods))

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for period := range jobs {
				if ctx.Err() != nil {
					return
				}
				orders, err := clearSinglePeriod(req.Orders, period)
				select {
				case results <- periodResult{Period: period, Orders: orders, Err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, period := range periods {
			select {
			case jobs <- period:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		if result.Err != nil {
			return Settlement{}, result.Err
		}
		settlement.Orders[fmt.Sprintf("%d", result.Period)] = result.Orders
	}

	return settlement, ctx.Err()
}

func clearSinglePeriod(allOrders []Order, period int) ([]Order, error) {
	var orders []Order
	for _, order := range allOrders {
		if order.Period == period {
			orders = append(orders, order)
		}
	}

	if len(orders) == 0 {
		return []Order{}, nil
	}

	buys := make([]pricedBlock, 0)
	sells := make([]pricedBlock, 0)
	originals := make(map[string]Order, len(orders))

	for _, order := range orders {
		originals[order.IDOrder] = order
		for blockIdx, block := range order.Blocks {
			if block.Amount <= 0 {
				continue
			}

			pb := pricedBlock{
				Order:     order,
				BlockIdx:  blockIdx,
				Price:     block.Price,
				Remaining: block.Amount,
			}

			switch strings.ToUpper(strings.TrimSpace(order.Type)) {
			case "BUY":
				buys = append(buys, pb)
			case "SELL":
				sells = append(sells, pb)
			default:
				return nil, fmt.Errorf("unsupported order type %q for order %s", order.Type, order.IDOrder)
			}
		}
	}

	sort.SliceStable(buys, func(i, j int) bool {
		if buys[i].Price == buys[j].Price {
			if buys[i].Order.IDOrder == buys[j].Order.IDOrder {
				return buys[i].BlockIdx < buys[j].BlockIdx
			}
			return buys[i].Order.IDOrder < buys[j].Order.IDOrder
		}
		return buys[i].Price > buys[j].Price
	})

	sort.SliceStable(sells, func(i, j int) bool {
		if sells[i].Price == sells[j].Price {
			if sells[i].Order.IDOrder == sells[j].Order.IDOrder {
				return sells[i].BlockIdx < sells[j].BlockIdx
			}
			return sells[i].Order.IDOrder < sells[j].Order.IDOrder
		}
		return sells[i].Price < sells[j].Price
	})

	allocations := make(map[string]map[int]int)
	allocate := func(orderID string, blockIdx int, amount int) {
		if amount <= 0 {
			return
		}
		byBlock, ok := allocations[orderID]
		if !ok {
			byBlock = map[int]int{}
			allocations[orderID] = byBlock
		}
		byBlock[blockIdx] += amount
	}

	buyIdx := 0
	sellIdx := 0
	for buyIdx < len(buys) && sellIdx < len(sells) {
		buy := &buys[buyIdx]
		sell := &sells[sellIdx]

		if buy.Price < sell.Price {
			break
		}

		matched := buy.Remaining
		if sell.Remaining < matched {
			matched = sell.Remaining
		}

		allocate(buy.Order.IDOrder, buy.BlockIdx, matched)
		allocate(sell.Order.IDOrder, sell.BlockIdx, matched)

		buy.Remaining -= matched
		sell.Remaining -= matched

		if buy.Remaining == 0 {
			buyIdx++
		}
		if sell.Remaining == 0 {
			sellIdx++
		}
	}

	matchedOrders := rebuildMatchedOrders(originals, allocations)
	return matchedOrders, nil
}

func rebuildMatchedOrders(originals map[string]Order, allocations map[string]map[int]int) []Order {
	if len(allocations) == 0 {
		return []Order{}
	}

	orderIDs := make([]string, 0, len(allocations))
	for orderID := range allocations {
		orderIDs = append(orderIDs, orderID)
	}
	sort.Strings(orderIDs)

	matchedOrders := make([]Order, 0, len(orderIDs))
	for _, orderID := range orderIDs {
		original, ok := originals[orderID]
		if !ok {
			continue
		}

		matchedBlocks := make([]Block, 0, len(original.Blocks))
		for blockIdx, block := range original.Blocks {
			matchedAmount := allocations[orderID][blockIdx]
			if matchedAmount <= 0 {
				continue
			}
			matchedBlocks = append(matchedBlocks, Block{
				Amount: matchedAmount,
				Price:  block.Price,
			})
		}

		if len(matchedBlocks) == 0 {
			continue
		}

		cloned := original
		cloned.Blocks = matchedBlocks
		matchedOrders = append(matchedOrders, cloned)
	}

	sort.SliceStable(matchedOrders, func(i, j int) bool {
		if matchedOrders[i].Type == matchedOrders[j].Type {
			return matchedOrders[i].IDOrder < matchedOrders[j].IDOrder
		}
		return matchedOrders[i].Type < matchedOrders[j].Type
	})

	return matchedOrders
}

func collectPeriods(expected []int, orders []Order) []int {
	seen := make(map[int]struct{}, len(expected))
	periods := make([]int, 0, len(expected))

	for _, period := range expected {
		if _, ok := seen[period]; ok {
			continue
		}
		seen[period] = struct{}{}
		periods = append(periods, period)
	}

	for _, order := range orders {
		if _, ok := seen[order.Period]; ok {
			continue
		}
		seen[order.Period] = struct{}{}
		periods = append(periods, order.Period)
	}

	sort.Ints(periods)
	return periods
}

func makeRange(start, end int) []int {
	if end < start {
		return nil
	}
	values := make([]int, 0, end-start+1)
	for i := start; i <= end; i++ {
		values = append(values, i)
	}
	return values
}

func (c *APIClient) GetEvents(ctx context.Context, afterID int64, eventType string, wait time.Duration) ([]apiEvent, error) {
	waitMS := int(wait / time.Millisecond)
	if waitMS < 0 {
		waitMS = 0
	}

	path := fmt.Sprintf("/events?after_id=%d&type=%s&wait_ms=%d", afterID, eventType, waitMS)
	var events []apiEvent
	if err := c.getJSON(ctx, path, "auctioneer", "oracle", &events); err != nil {
		return nil, err
	}
	return events, nil
}

func (c *APIClient) getJSON(ctx context.Context, path string, role string, actorID string, out any) error {
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

func (c *APIClient) postJSON(ctx context.Context, path string, role string, actorID string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(data))
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

func NewFileCheckpoint(path string) (*FileCheckpoint, error) {
	cp := &FileCheckpoint{path: path}
	if err := cp.load(); err != nil {
		return nil, err
	}
	return cp, nil
}

func (c *FileCheckpoint) load() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := os.ReadFile(c.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.last = 0
			return nil
		}
		return err
	}

	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		c.last = 0
		return nil
	}

	v, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid checkpoint content %q: %w", trimmed, err)
	}
	if v < 0 {
		return fmt.Errorf("invalid checkpoint value %d", v)
	}
	c.last = v
	return nil
}

func (c *FileCheckpoint) Last() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

func (c *FileCheckpoint) Save(value int64) error {
	if value < 0 {
		return fmt.Errorf("invalid checkpoint value %d", value)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if value <= c.last {
		return nil
	}
	if err := os.WriteFile(c.path, []byte(fmt.Sprintf("%d\n", value)), 0o644); err != nil {
		return err
	}
	c.last = value
	return nil
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
