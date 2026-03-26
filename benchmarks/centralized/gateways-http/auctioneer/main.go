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
	"strings"
	"sync"
	"syscall"
	"time"
)

const marketDateLayout = "02-01-2006"

type Config struct {
	APIBaseURL    string
	Timezone      string
	PollInterval  time.Duration
	RequestTimout time.Duration
	LogLevel      slog.Level
}

type Market struct {
	ID       string   `json:"id"`
	Status   string   `json:"status"`
	Auctions []string `json:"auctions"`
}

type ActionJournal struct {
	mu   sync.Mutex
	done map[string]struct{}
}

type APIClient struct {
	baseURL string
	http    *http.Client
}

type Daemon struct {
	cfg     Config
	loc     *time.Location
	client  *APIClient
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

	daemon := &Daemon{
		cfg: cfg,
		loc: loc,
		client: &APIClient{
			baseURL: strings.TrimRight(cfg.APIBaseURL, "/"),
			http: &http.Client{
				Timeout: cfg.RequestTimout,
			},
		},
		journal: &ActionJournal{done: map[string]struct{}{}},
		logger:  logger,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("http auctioneer daemon started",
		"api", cfg.APIBaseURL,
		"timezone", cfg.Timezone,
		"poll_interval", cfg.PollInterval.String(),
	)

	if err := daemon.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("auctioneer daemon stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("http auctioneer daemon stopped")
}

func loadConfig() (Config, error) {
	cfg := Config{
		APIBaseURL: envOrDefault("APP_API_BASE_URL", "http://127.0.0.1:8080"),
		Timezone:   envOrDefault("APP_TIMEZONE", "Europe/Madrid"),
		LogLevel:   parseLogLevel(envOrDefault("APP_LOG_LEVEL", "INFO")),
	}

	var err error
	cfg.PollInterval, err = parseDurationEnv("APP_POLL_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.RequestTimout, err = parseDurationEnv("APP_REQUEST_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}

	if strings.TrimSpace(cfg.APIBaseURL) == "" {
		return Config{}, fmt.Errorf("APP_API_BASE_URL is required")
	}

	return cfg, nil
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

	ctx, cancel := context.WithTimeout(parent, 2*d.cfg.RequestTimout)
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

func (c *APIClient) GetMarkets(ctx context.Context) ([]Market, error) {
	var markets []Market
	if err := c.getJSON(ctx, "/markets", &markets); err != nil {
		return nil, err
	}
	sort.Slice(markets, func(i, j int) bool {
		return markets[i].ID < markets[j].ID
	})
	return markets, nil
}

func (c *APIClient) CreateMarket(ctx context.Context, marketID string) error {
	return c.postJSON(ctx, "/markets", map[string]string{"date": marketID}, nil)
}

func (c *APIClient) OpenAuction(ctx context.Context, auctionID string) error {
	return c.postJSON(ctx, "/auctions/"+auctionID+"/open", nil, nil)
}

func (c *APIClient) CloseAuction(ctx context.Context, auctionID string) error {
	return c.postJSON(ctx, "/auctions/"+auctionID+"/close", nil, nil)
}

func (c *APIClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Actor-Role", "auctioneer")

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

func (c *APIClient) postJSON(ctx context.Context, path string, body any, out any) error {
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
	req.Header.Set("X-Actor-Role", "auctioneer")
	req.Header.Set("Content-Type", "application/json")

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
