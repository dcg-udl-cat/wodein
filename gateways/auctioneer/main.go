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

const marketDateLayout = "02-01-2006"

type Config struct {
	ChannelName         string
	ChaincodeName       string
	MSPID               string
	PeerEndpoint        string
	PeerTLSHostOverride string
	ClientCertPath      string
	ClientKeyPath       string
	PeerTLSRootCertPath string
	Timezone            string
	PollInterval        time.Duration
	EvaluateTimeout     time.Duration
	EndorseTimeout      time.Duration
	SubmitTimeout       time.Duration
	CommitStatusTimeout time.Duration
	LogLevel            slog.Level
}

type Market struct {
	ID       string   `json:"id"`
	Status   string   `json:"status"`
	Auctions []string `json:"auctions"`
}

type FabricClient struct {
	gateway  *client.Gateway
	grpcConn *grpc.ClientConn
	contract *client.Contract
	logger   *slog.Logger
}

type ActionJournal struct {
	mu   sync.Mutex
	done map[string]struct{}
}

type Daemon struct {
	cfg     Config
	loc     *time.Location
	client  *FabricClient
	journal *ActionJournal
	logger  *slog.Logger
}

type Session struct {
	AuctionID string
	MarketID  string
	Label     string
	OpenAt    time.Time
	CloseAt   time.Time
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

	fabricClient, err := newFabricClient(cfg, logger)
	if err != nil {
		logger.Error("failed to connect to Fabric gateway", "error", err)
		os.Exit(1)
	}
	defer fabricClient.Close()

	daemon := &Daemon{
		cfg:     cfg,
		loc:     loc,
		client:  fabricClient,
		journal: &ActionJournal{done: map[string]struct{}{}},
		logger:  logger,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("auctioneer daemon started",
		"channel", cfg.ChannelName,
		"chaincode", cfg.ChaincodeName,
		"peer", cfg.PeerEndpoint,
		"timezone", cfg.Timezone,
		"poll_interval", cfg.PollInterval.String(),
	)

	if err := daemon.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("auctioneer daemon stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("auctioneer daemon stopped")
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
		Timezone:            envOrDefault("APP_TIMEZONE", "Europe/Madrid"),
		LogLevel:            parseLogLevel(envOrDefault("APP_LOG_LEVEL", "INFO")),
	}

	var err error
	cfg.PollInterval, err = parseDurationEnv("APP_POLL_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
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
	cfg.CommitStatusTimeout, err = parseDurationEnv("FABRIC_COMMIT_STATUS_TIMEOUT", 1*time.Minute)
	if err != nil {
		return Config{}, err
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

func newFabricClient(cfg Config, logger *slog.Logger) (*FabricClient, error) {
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
		contract: contract,
		logger:   logger,
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

func (c *FabricClient) GetMarkets(ctx context.Context) ([]Market, error) {
	result, err := c.evaluate(ctx, "GetMarkets")
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return []Market{}, nil
	}

	var markets []Market
	if err := json.Unmarshal(result, &markets); err != nil {
		return nil, fmt.Errorf("decode GetMarkets response: %w", err)
	}

	sort.Slice(markets, func(i, j int) bool {
		return markets[i].ID < markets[j].ID
	})

	return markets, nil
}

func (c *FabricClient) CreateMarket(ctx context.Context, marketID string) error {
	_, err := c.submit(ctx, "CreateMarket", marketID)
	return err
}

func (c *FabricClient) OpenAuction(ctx context.Context, auctionID string) error {
	_, err := c.submit(ctx, "OpenAuction", auctionID)
	return err
}

func (c *FabricClient) CloseAuction(ctx context.Context, auctionID string) error {
	_, err := c.submit(ctx, "CloseAuction", auctionID)
	return err
}

func (c *FabricClient) evaluate(ctx context.Context, name string, args ...string) ([]byte, error) {
	result, err := c.contract.EvaluateWithContext(ctx, name, client.WithArguments(args...))
	if err != nil {
		return nil, fmt.Errorf("evaluate %s failed: %w", name, err)
	}
	return result, nil
}

func (c *FabricClient) submit(ctx context.Context, name string, args ...string) ([]byte, error) {
	result, err := c.contract.SubmitWithContext(ctx, name, client.WithArguments(args...))
	if err != nil {
		return nil, fmt.Errorf("submit %s failed: %w", name, err)
	}
	return result, nil
}

func (d *Daemon) Run(ctx context.Context) error {
	if err := d.reconcile(ctx); err != nil {
		d.logger.Error("initial reconcile failed", "error", err)
	}

	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.reconcile(ctx); err != nil {
				d.logger.Error("reconcile failed", "error", err)
			}
		}
	}
}

func (d *Daemon) reconcile(parent context.Context) error {
	now := time.Now().In(d.loc)
	d.logger.Info("reconcile started", "now", now.Format(time.RFC3339))

	ctx, cancel := context.WithTimeout(parent, 2*d.cfg.CommitStatusTimeout)
	defer cancel()

	markets, err := d.client.GetMarkets(ctx)
	if err != nil {
		return err
	}

	if err := d.reconcileExistingMarkets(ctx, now, markets); err != nil {
		return err
	}

	markets, err = d.client.GetMarkets(ctx)
	if err != nil {
		return err
	}

	if err := d.ensureTomorrowMarketCreated(ctx, now, markets); err != nil {
		return err
	}

	markets, err = d.client.GetMarkets(ctx)
	if err != nil {
		return err
	}

	if err := d.reconcileExistingMarkets(ctx, now, markets); err != nil {
		return err
	}

	d.logger.Info("reconcile completed", "markets", len(markets))
	return nil
}

func (d *Daemon) reconcileExistingMarkets(ctx context.Context, now time.Time, markets []Market) error {
	for _, market := range markets {
		marketDay, err := time.ParseInLocation(marketDateLayout, market.ID, d.loc)
		if err != nil {
			d.logger.Warn("skipping market with invalid date id", "market_id", market.ID, "error", err)
			continue
		}

		for _, session := range buildSessionsForMarket(marketDay) {
			switch {
			case !now.Before(session.CloseAt):
				if err := d.ensureClosed(ctx, session); err != nil {
					return err
				}
			case !now.Before(session.OpenAt):
				if err := d.ensureOpen(ctx, session); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (d *Daemon) ensureTomorrowMarketCreated(ctx context.Context, now time.Time, markets []Market) error {
	createAt := time.Date(now.Year(), now.Month(), now.Day(), 11, 0, 0, 0, d.loc)
	if now.Before(createAt) {
		return nil
	}

	tomorrowID := now.AddDate(0, 0, 1).Format(marketDateLayout)
	actionKey := "create:" + tomorrowID

	if marketExists(markets, tomorrowID) {
		d.journal.MarkDone(actionKey)
		return nil
	}
	if d.journal.IsDone(actionKey) {
		return nil
	}

	d.logger.Info("creating tomorrow market", "market_id", tomorrowID)
	err := d.client.CreateMarket(ctx, tomorrowID)
	if err != nil {
		if isMarketAlreadyExistsError(err) {
			d.journal.MarkDone(actionKey)
			d.logger.Info("market already exists", "market_id", tomorrowID)
			return nil
		}
		return err
	}

	d.journal.MarkDone(actionKey)
	d.logger.Info("market created", "market_id", tomorrowID)
	return nil
}

func (d *Daemon) ensureOpen(ctx context.Context, session Session) error {
	actionKey := "open:" + session.AuctionID
	if d.journal.IsDone(actionKey) {
		return nil
	}

	d.logger.Info("opening auction session",
		"auction_id", session.AuctionID,
		"market_id", session.MarketID,
		"session", session.Label,
		"open_at", session.OpenAt.Format(time.RFC3339),
		"close_at", session.CloseAt.Format(time.RFC3339),
	)

	err := d.client.OpenAuction(ctx, session.AuctionID)
	if err != nil {
		if isAuctionAlreadyOpenError(err) {
			d.journal.MarkDone(actionKey)
			d.logger.Info("auction already open", "auction_id", session.AuctionID)
			return nil
		}
		return err
	}

	d.journal.MarkDone(actionKey)
	d.logger.Info("auction opened", "auction_id", session.AuctionID)
	return nil
}

func (d *Daemon) ensureClosed(ctx context.Context, session Session) error {
	actionKey := "close:" + session.AuctionID
	if d.journal.IsDone(actionKey) {
		return nil
	}

	d.logger.Info("closing auction session",
		"auction_id", session.AuctionID,
		"market_id", session.MarketID,
		"session", session.Label,
		"close_at", session.CloseAt.Format(time.RFC3339),
	)

	err := d.client.CloseAuction(ctx, session.AuctionID)
	if err != nil {
		if isAuctionAlreadyClosedError(err) {
			d.journal.MarkDone(actionKey)
			d.logger.Info("auction already closed", "auction_id", session.AuctionID)
			return nil
		}
		return err
	}

	d.journal.MarkDone(actionKey)
	d.logger.Info("auction closed", "auction_id", session.AuctionID)
	return nil
}

func buildSessionsForMarket(marketDay time.Time) []Session {
	marketID := marketDay.Format(marketDateLayout)
	previousDay := marketDay.AddDate(0, 0, -1)

	return []Session{
		{
			AuctionID: marketID + "-A1",
			MarketID:  marketID,
			Label:     "session_1",
			OpenAt:    time.Date(previousDay.Year(), previousDay.Month(), previousDay.Day(), 14, 0, 0, 0, marketDay.Location()),
			CloseAt:   time.Date(previousDay.Year(), previousDay.Month(), previousDay.Day(), 15, 0, 0, 0, marketDay.Location()),
		},
		{
			AuctionID: marketID + "-A2",
			MarketID:  marketID,
			Label:     "session_2",
			OpenAt:    time.Date(previousDay.Year(), previousDay.Month(), previousDay.Day(), 21, 0, 0, 0, marketDay.Location()),
			CloseAt:   time.Date(previousDay.Year(), previousDay.Month(), previousDay.Day(), 22, 0, 0, 0, marketDay.Location()),
		},
		{
			AuctionID: marketID + "-A3",
			MarketID:  marketID,
			Label:     "session_3",
			OpenAt:    time.Date(marketDay.Year(), marketDay.Month(), marketDay.Day(), 9, 0, 0, 0, marketDay.Location()),
			CloseAt:   time.Date(marketDay.Year(), marketDay.Month(), marketDay.Day(), 10, 0, 0, 0, marketDay.Location()),
		},
	}
}

func marketExists(markets []Market, marketID string) bool {
	for _, market := range markets {
		if market.ID == marketID {
			return true
		}
	}
	return false
}

func (j *ActionJournal) IsDone(key string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	_, ok := j.done[key]
	return ok
}

func (j *ActionJournal) MarkDone(key string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.done[key] = struct{}{}
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
