package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
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

type Config struct {
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
	CheckpointFile      string
	ReconnectDelay      time.Duration
	MaxParallelPeriods  int
	LogLevel            slog.Level
}

type CloseAuctionEvent struct {
	IDAuction string `json:"id_auction"`
}

type Block struct {
	Amount int     `json:"amount"`
	Price  float64 `json:"price"`
	Status string  `json:"status,omitempty"`
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

type FabricClient struct {
	gateway  *client.Gateway
	grpcConn *grpc.ClientConn
	network  *client.Network
	contract *client.Contract
}

type FabricLedger struct {
	contract *client.Contract
}

type SettlementService struct {
	ledger   LedgerGateway
	resolver AuctionContextResolver
	clearers map[string]ClearingAlgorithm
	current  string
	logger   *slog.Logger
}

type SettlementListener struct {
	cfg          Config
	logger       *slog.Logger
	network      *client.Network
	checkpointer *client.FileCheckpointer
	processor    *SettlementService
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

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	fabricClient, err := newFabricClient(cfg)
	if err != nil {
		logger.Error("failed to connect to Fabric gateway", "error", err)
		os.Exit(1)
	}
	defer fabricClient.Close()

	checkpointer, err := client.NewFileCheckpointer(cfg.CheckpointFile)
	if err != nil {
		logger.Error("failed to create file checkpointer", "path", cfg.CheckpointFile, "error", err)
		os.Exit(1)
	}
	defer checkpointer.Close()

	ledger := &FabricLedger{contract: fabricClient.contract}

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
		cfg:          cfg,
		logger:       logger,
		network:      fabricClient.network,
		checkpointer: checkpointer,
		processor:    settlementService,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info(
		"settlement listener started",
		"channel", cfg.ChannelName,
		"chaincode", cfg.ChaincodeName,
		"peer", cfg.PeerEndpoint,
		"checkpoint_file", cfg.CheckpointFile,
		"clearing_algorithm", settlementService.current,
	)

	if err := listener.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("settlement listener stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("settlement listener stopped")
}

func loadConfig() (Config, error) {
	cfg := Config{
		ChannelName:         envOrDefault("FABRIC_CHANNEL_NAME", "mychannel"),
		ChaincodeName:       envOrDefault("FABRIC_CHAINCODE_NAME", "market"),
		MSPID:               envOrDefault("FABRIC_MSP_ID", "Org1MSP"),
		PeerEndpoint:        strings.TrimSpace(envOrDefault("FABRIC_PEER_ENDPOINT", "dns:///localhost:7051")),
		PeerTLSHostOverride: strings.TrimSpace(envOrDefault("FABRIC_PEER_TLS_HOST_OVERRIDE", "peer0.org1.example.com")),
		ClientCertPath:      strings.TrimSpace(envOrDefault("FABRIC_CLIENT_CERT_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/signcerts")),
		ClientKeyPath:       strings.TrimSpace(envOrDefault("FABRIC_CLIENT_KEY_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/keystore")),
		PeerTLSRootCertPath: strings.TrimSpace(envOrDefault("FABRIC_PEER_TLS_ROOTCERT_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt")),
		CheckpointFile:      envOrDefault("APP_CHECKPOINT_FILE", "./close-auction.checkpoint"),
		LogLevel:            parseLogLevel(envOrDefault("APP_LOG_LEVEL", "INFO")),
	}

	var err error
	cfg.EvaluateTimeout, err = parseDurationEnv("FABRIC_EVALUATE_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.EndorseTimeout, err = parseDurationEnv("FABRIC_ENDORSE_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.SubmitTimeout, err = parseDurationEnv("FABRIC_SUBMIT_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.CommitStatusTimeout, err = parseDurationEnv("FABRIC_COMMIT_STATUS_TIMEOUT", time.Minute)
	if err != nil {
		return Config{}, err
	}
	cfg.ReconnectDelay, err = parseDurationEnv("APP_RECONNECT_DELAY", 3*time.Second)
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

	missing := make([]string, 0, 4)
	if cfg.PeerEndpoint == "" {
		missing = append(missing, "FABRIC_PEER_ENDPOINT")
	}
	if cfg.ClientCertPath == "" {
		missing = append(missing, "FABRIC_CLIENT_CERT_PATH")
	}
	if cfg.ClientKeyPath == "" {
		missing = append(missing, "FABRIC_CLIENT_KEY_PATH")
	}
	if cfg.PeerTLSRootCertPath == "" {
		missing = append(missing, "FABRIC_PEER_TLS_ROOTCERT_PATH")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	return cfg, nil
}

func newFabricClient(cfg Config) (*FabricClient, error) {
	grpcConn, err := newGRPCConnection(cfg)
	if err != nil {
		return nil, err
	}

	id, err := newIdentity(cfg)
	if err != nil {
		grpcConn.Close()
		return nil, err
	}

	sign, err := newSign(cfg)
	if err != nil {
		grpcConn.Close()
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
		grpcConn.Close()
		return nil, fmt.Errorf("connect gateway: %w", err)
	}

	network := gateway.GetNetwork(cfg.ChannelName)
	contract := network.GetContract(cfg.ChaincodeName)

	return &FabricClient{
		gateway:  gateway,
		grpcConn: grpcConn,
		network:  network,
		contract: contract,
	}, nil
}

func (c *FabricClient) Close() {
	if c.gateway != nil {
		c.gateway.Close()
	}
	if c.grpcConn != nil {
		_ = c.grpcConn.Close()
	}
}

func newGRPCConnection(cfg Config) (*grpc.ClientConn, error) {
	const maxMsgSize = 50 * 1024 * 1024 // 50MB
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

	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create gRPC client for %s: %w", cfg.PeerEndpoint, err)
	}

	return conn, nil
}

func newIdentity(cfg Config) (*identity.X509Identity, error) {
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

func newSign(cfg Config) (identity.Sign, error) {
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

func (l *SettlementListener) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		streamCtx, cancel := context.WithCancel(ctx)
		events, err := l.network.ChaincodeEvents(
			streamCtx,
			l.cfg.ChaincodeName,
			client.WithCheckpoint(l.checkpointer),
		)
		if err != nil {
			cancel()
			l.logger.Error("failed to subscribe to chaincode events", "error", err)
			if err := sleepWithContext(ctx, l.cfg.ReconnectDelay); err != nil {
				return err
			}
			continue
		}

		l.logger.Info(
			"waiting for chaincode events",
			"chaincode", l.cfg.ChaincodeName,
			"next_block", l.checkpointer.BlockNumber(),
		)

		reconnect := false

		for event := range events {
			if event == nil {
				continue
			}

			if event.EventName != "CloseAuction" {
				if err := l.checkpointEvent(event); err != nil {
					reconnect = true
					cancel()
					break
				}
				continue
			}

			if err := l.handleCloseAuctionEvent(streamCtx, event); err != nil {
				l.logger.Error(
					"failed to process CloseAuction event",
					"block_number", event.BlockNumber,
					"transaction_id", event.TransactionID,
					"error", err,
				)
				reconnect = true
				cancel()
				break
			}

			if err := l.checkpointEvent(event); err != nil {
				reconnect = true
				cancel()
				break
			}
		}

		cancel()

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if reconnect {
			if err := sleepWithContext(ctx, l.cfg.ReconnectDelay); err != nil {
				return err
			}
			continue
		}

		l.logger.Warn("event stream closed, reconnecting")
		if err := sleepWithContext(ctx, l.cfg.ReconnectDelay); err != nil {
			return err
		}
	}
}

func (l *SettlementListener) handleCloseAuctionEvent(ctx context.Context, event *client.ChaincodeEvent) error {
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
		"block_number", event.BlockNumber,
		"transaction_id", event.TransactionID,
	)

	if err := l.processor.ProcessAuctionClosed(ctx, payload.IDAuction); err != nil {
		return err
	}

	l.logger.Info(
		"settlement published",
		"auction_id", payload.IDAuction,
		"block_number", event.BlockNumber,
		"transaction_id", event.TransactionID,
	)

	return nil
}

func (l *SettlementListener) checkpointEvent(event *client.ChaincodeEvent) error {
	if err := l.checkpointer.CheckpointChaincodeEvent(event); err != nil {
		return fmt.Errorf("checkpoint event %s in block %d: %w", event.TransactionID, event.BlockNumber, err)
	}
	if err := l.checkpointer.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint for event %s in block %d: %w", event.TransactionID, event.BlockNumber, err)
	}
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

func (l *FabricLedger) GetOrdersByAuction(ctx context.Context, auctionID string) ([]Order, error) {
	result, err := l.contract.EvaluateWithContext(ctx, "GetOrdersByAuction", client.WithArguments(auctionID))
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return []Order{}, nil
	}

	var orders []Order
	if err := json.Unmarshal(result, &orders); err != nil {
		return nil, fmt.Errorf("decode GetOrdersByAuction response: %w", err)
	}

	return orders, nil
}

func (l *FabricLedger) UpdateSettlement(ctx context.Context, marketID string, settlement Settlement) error {
	payload, err := json.Marshal(settlement)
	if err != nil {
		return fmt.Errorf("marshal settlement: %w", err)
	}

	_, err = l.contract.SubmitWithContext(ctx, "UpdateSettlement", client.WithArguments(marketID, string(payload)))
	if err != nil {
		return err
	}

	return nil
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

		blocksWithStatus := make([]Block, 0, len(original.Blocks))
		for blockIdx, block := range original.Blocks {
			matchedAmount := allocations[orderID][blockIdx]
			status := "NOT_USED"
			if matchedAmount > 0 {
				status = "USED"
			}
			blocksWithStatus = append(blocksWithStatus, Block{
				Amount: block.Amount,
				Price:  block.Price,
				Status: status,
			})
		}

		// Preserve orders even if nothing matched so that unused blocks are visible.
		cloned := original
		cloned.Blocks = blocksWithStatus
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
