package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

const (
	statusNewlyCreated = "NEWLY_CREATED"
	statusOpen         = "OPEN"
	statusClosed       = "CLOSED"

	orderTypeBuy  = "BUY"
	orderTypeSell = "SELL"

	marketDateLayout = "02-01-2006"
)

type Config struct {
	ListenAddr string
	DBPath     string
	LogLevel   slog.Level
}

type App struct {
	db     *sql.DB
	logger *slog.Logger
}

type Market struct {
	ID       string   `json:"id"`
	Status   string   `json:"status"`
	Auctions []string `json:"auctions"`
}

type Auction struct {
	ID          string `json:"id"`
	IDMarket    string `json:"id_market"`
	Status      string `json:"status"`
	PeriodBegin int    `json:"period_begin"`
	PeriodEnd   int    `json:"period_end"`
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

type closeAuctionEvent struct {
	IDAuction string `json:"id_auction"`
}

type event struct {
	ID        int64           `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt string          `json:"created_at"`
}

type createMarketRequest struct {
	Date string `json:"date"`
}

type addOrderRequest struct {
	IDAuction string  `json:"id_auction"`
	Period    int     `json:"period"`
	Type      string  `json:"type"`
	Blocks    []Block `json:"blocks"`
	ClientID  string  `json:"client_id"`
}

type addVerityRequest struct {
	OrderID string `json:"order_id"`
	Hash    string `json:"hash"`
}

func main() {
	cfg := loadConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if err := initSchema(db); err != nil {
		logger.Error("failed to initialize schema", "error", err)
		os.Exit(1)
	}

	app := &App{db: db, logger: logger}
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		Handler:           app.routes(),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("db market api started", "addr", cfg.ListenAddr, "db", cfg.DBPath)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("db market api stopped")
}

func loadConfig() Config {
	return Config{
		ListenAddr: envOrDefault("APP_LISTEN_ADDR", ":8080"),
		DBPath:     envOrDefault("APP_DB_PATH", "./market.db"),
		LogLevel:   parseLogLevel(envOrDefault("APP_LOG_LEVEL", "INFO")),
	}
}

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.handleHealthz)
	mux.HandleFunc("GET /markets", a.handleGetMarkets)
	mux.HandleFunc("POST /markets", a.handleCreateMarket)
	mux.HandleFunc("GET /markets/open", a.handleGetOpenedMarket)
	mux.HandleFunc("GET /auctions/current", a.handleGetCurrentAuction)
	mux.HandleFunc("POST /auctions/", a.handleAuctionAction)
	mux.HandleFunc("GET /auctions/", a.handleGetOrdersByAuction)
	mux.HandleFunc("POST /orders", a.handleAddOrder)
	mux.HandleFunc("POST /verity", a.handleAddVerity)
	mux.HandleFunc("GET /settlements/", a.handleGetSettlement)
	mux.HandleFunc("POST /settlements/", a.handleUpdateSettlement)
	mux.HandleFunc("GET /events", a.handleGetEvents)
	return mux
}

func (a *App) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *App) handleCreateMarket(w http.ResponseWriter, r *http.Request) {
	if !requireAuctioneer(w, r) {
		return
	}

	var req createMarketRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if err := a.createMarket(r.Context(), strings.TrimSpace(req.Date)); err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"market_id": req.Date})
}

func (a *App) handleGetMarkets(w http.ResponseWriter, r *http.Request) {
	markets, err := a.getMarkets(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, markets)
}

func (a *App) handleGetOpenedMarket(w http.ResponseWriter, r *http.Request) {
	market, err := a.getOpenedMarket(r.Context())
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, market)
}

func (a *App) handleGetCurrentAuction(w http.ResponseWriter, r *http.Request) {
	auction, err := a.getCurrentAuction(r.Context())
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, auction)
}

func (a *App) handleAuctionAction(w http.ResponseWriter, r *http.Request) {
	if !requireAuctioneer(w, r) {
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/auctions/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	auctionID := strings.TrimSpace(parts[0])
	action := strings.TrimSpace(parts[1])

	var err error
	switch action {
	case "open":
		err = a.openAuction(r.Context(), auctionID)
	case "close":
		err = a.closeAuction(r.Context(), auctionID)
	default:
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	if err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *App) handleAddOrder(w http.ResponseWriter, r *http.Request) {
	var req addOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	actorID := strings.TrimSpace(r.Header.Get("X-Actor-ID"))
	if actorID == "" {
		actorID = strings.TrimSpace(req.ClientID)
	}
	if actorID == "" {
		actorID = "anonymous"
	}

	orderID, err := a.addOrder(r.Context(), req, actorID)
	if err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"order_id": orderID})
}

func (a *App) handleGetOrdersByAuction(w http.ResponseWriter, r *http.Request) {
	if !requireAuctioneer(w, r) {
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/auctions/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[1] != "orders" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	orders, err := a.getOrdersByAuction(r.Context(), parts[0])
	if err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, orders)
}

func (a *App) handleAddVerity(w http.ResponseWriter, r *http.Request) {
	var req addVerityRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	if err := a.addVerity(r.Context(), req.OrderID, req.Hash); err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"status": "ok"})
}

func (a *App) handleGetSettlement(w http.ResponseWriter, r *http.Request) {
	marketID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/settlements/"))
	settlement, err := a.getSettlement(r.Context(), marketID)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, settlement)
}

func (a *App) handleUpdateSettlement(w http.ResponseWriter, r *http.Request) {
	if !requireAuctioneer(w, r) {
		return
	}
	marketID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/settlements/"))
	if marketID == "" || strings.Contains(marketID, "/") {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	var incoming Settlement
	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if err := a.updateSettlement(r.Context(), marketID, incoming); err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *App) handleGetEvents(w http.ResponseWriter, r *http.Request) {
	afterID := int64(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("after_id")); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			writeError(w, http.StatusBadRequest, "after_id must be a non-negative integer")
			return
		}
		afterID = v
	}

	eventType := strings.TrimSpace(r.URL.Query().Get("type"))
	waitMS := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("wait_ms")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 || v > 30000 {
			writeError(w, http.StatusBadRequest, "wait_ms must be an integer between 0 and 30000")
			return
		}
		waitMS = v
	}

	ctx := r.Context()
	deadline := time.Now().Add(time.Duration(waitMS) * time.Millisecond)
	for {
		events, err := a.getEvents(ctx, afterID, eventType, 200)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(events) > 0 || waitMS == 0 || time.Now().After(deadline) {
			writeJSON(w, http.StatusOK, events)
			return
		}
		if err := sleepWithContext(ctx, 250*time.Millisecond); err != nil {
			writeError(w, http.StatusRequestTimeout, "request canceled")
			return
		}
	}
}

func (a *App) createMarket(ctx context.Context, date string) error {
	if date == "" {
		return fmt.Errorf("date is required")
	}
	if _, err := time.Parse(marketDateLayout, date); err != nil {
		return fmt.Errorf("invalid date format %q, expected dd-mm-yyyy", date)
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var existing int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(1) FROM markets WHERE id = ?", date).Scan(&existing); err != nil {
		return err
	}
	if existing > 0 {
		return fmt.Errorf("market %s already exists", date)
	}

	var activeCount int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(1) FROM markets WHERE status IN (?, ?)", statusNewlyCreated, statusOpen).Scan(&activeCount); err != nil {
		return err
	}
	if activeCount > 0 {
		return fmt.Errorf("cannot create market %s: there is already a %s or %s market", date, statusNewlyCreated, statusOpen)
	}

	if _, err := tx.ExecContext(ctx, "INSERT INTO markets(id, status, created_at) VALUES(?, ?, ?)", date, statusNewlyCreated, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}

	auctions := []Auction{
		{ID: date + "-A1", IDMarket: date, Status: statusNewlyCreated, PeriodBegin: 1, PeriodEnd: 24},
		{ID: date + "-A2", IDMarket: date, Status: statusNewlyCreated, PeriodBegin: 1, PeriodEnd: 24},
		{ID: date + "-A3", IDMarket: date, Status: statusNewlyCreated, PeriodBegin: 13, PeriodEnd: 24},
	}
	for _, auction := range auctions {
		_, err := tx.ExecContext(ctx,
			"INSERT INTO auctions(id, market_id, status, period_begin, period_end, created_at) VALUES(?, ?, ?, ?, ?, ?)",
			auction.ID,
			auction.IDMarket,
			auction.Status,
			auction.PeriodBegin,
			auction.PeriodEnd,
			time.Now().UTC().Format(time.RFC3339Nano),
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (a *App) getMarkets(ctx context.Context) ([]Market, error) {
	rows, err := a.db.QueryContext(ctx, "SELECT id, status FROM markets ORDER BY id ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	markets := make([]Market, 0)
	for rows.Next() {
		var market Market
		if err := rows.Scan(&market.ID, &market.Status); err != nil {
			return nil, err
		}
		markets = append(markets, market)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for idx := range markets {
		auctionRows, err := a.db.QueryContext(ctx, "SELECT id FROM auctions WHERE market_id = ? ORDER BY id ASC", markets[idx].ID)
		if err != nil {
			return nil, err
		}

		var auctionIDs []string
		for auctionRows.Next() {
			var auctionID string
			if err := auctionRows.Scan(&auctionID); err != nil {
				auctionRows.Close()
				return nil, err
			}
			auctionIDs = append(auctionIDs, auctionID)
		}
		auctionRows.Close()
		markets[idx].Auctions = auctionIDs
	}

	return markets, nil
}

func (a *App) getOpenedMarket(ctx context.Context) (*Market, error) {
	markets, err := a.getMarkets(ctx)
	if err != nil {
		return nil, err
	}

	for _, market := range markets {
		if market.Status == statusOpen {
			copyMarket := market
			return &copyMarket, nil
		}
	}

	return nil, fmt.Errorf("no OPEN market found")
}

func (a *App) getCurrentAuction(ctx context.Context) (*Auction, error) {
	market, err := a.getOpenedMarket(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := a.db.QueryContext(ctx, "SELECT id, market_id, status, period_begin, period_end FROM auctions WHERE market_id = ? AND status = ? ORDER BY period_begin ASC, id ASC", market.ID, statusOpen)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var opened []Auction
	for rows.Next() {
		var auction Auction
		if err := rows.Scan(&auction.ID, &auction.IDMarket, &auction.Status, &auction.PeriodBegin, &auction.PeriodEnd); err != nil {
			return nil, err
		}
		opened = append(opened, auction)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(opened) == 0 {
		return nil, fmt.Errorf("no OPEN auction found in market %s", market.ID)
	}

	current := opened[0]
	return &current, nil
}

func (a *App) openAuction(ctx context.Context, auctionID string) error {
	auction, err := a.getAuction(ctx, auctionID)
	if err != nil {
		return err
	}
	if auction.Status != statusNewlyCreated {
		return fmt.Errorf("auction %s must be %s to open, current status: %s", auctionID, statusNewlyCreated, auction.Status)
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "UPDATE auctions SET status = ? WHERE id = ?", statusOpen, auctionID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE markets SET status = ? WHERE id = ? AND status = ?", statusOpen, auction.IDMarket, statusNewlyCreated); err != nil {
		return err
	}

	return tx.Commit()
}

func (a *App) closeAuction(ctx context.Context, auctionID string) error {
	auction, err := a.getAuction(ctx, auctionID)
	if err != nil {
		return err
	}
	if auction.Status == statusClosed {
		return fmt.Errorf("auction %s is already CLOSED", auctionID)
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "UPDATE auctions SET status = ? WHERE id = ?", statusClosed, auctionID); err != nil {
		return err
	}

	var notClosed int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(1) FROM auctions WHERE market_id = ? AND status <> ?", auction.IDMarket, statusClosed).Scan(&notClosed); err != nil {
		return err
	}
	if notClosed == 0 {
		if _, err := tx.ExecContext(ctx, "UPDATE markets SET status = ? WHERE id = ?", statusClosed, auction.IDMarket); err != nil {
			return err
		}
	}

	payload, err := json.Marshal(closeAuctionEvent{IDAuction: auctionID})
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO events(event_type, payload, created_at) VALUES(?, ?, ?)", "CloseAuction", string(payload), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}

	return tx.Commit()
}

func (a *App) addOrder(ctx context.Context, req addOrderRequest, actorID string) (string, error) {
	req.IDAuction = strings.TrimSpace(req.IDAuction)
	if req.IDAuction == "" {
		return "", fmt.Errorf("auction_id is required")
	}
	if req.Period < 1 || req.Period > 24 {
		return "", fmt.Errorf("period must be between 1 and 24")
	}

	req.Type = strings.ToUpper(strings.TrimSpace(req.Type))
	if req.Type != orderTypeBuy && req.Type != orderTypeSell {
		return "", fmt.Errorf("type must be BUY or SELL")
	}
	if err := validateBlocks(req.Blocks); err != nil {
		return "", err
	}

	auction, err := a.getAuction(ctx, req.IDAuction)
	if err != nil {
		return "", err
	}
	if auction.Status != statusOpen {
		return "", fmt.Errorf("auction %s is not OPEN", req.IDAuction)
	}

	market, err := a.getMarket(ctx, auction.IDMarket)
	if err != nil {
		return "", err
	}
	if market.Status != statusOpen {
		return "", fmt.Errorf("market %s is not OPEN", market.ID)
	}

	if req.Period < auction.PeriodBegin || req.Period > auction.PeriodEnd {
		return "", fmt.Errorf("period %d is outside auction %s range [%d, %d]", req.Period, req.IDAuction, auction.PeriodBegin, auction.PeriodEnd)
	}

	blocksJSON, err := json.Marshal(req.Blocks)
	if err != nil {
		return "", err
	}

	orderID, err := newOrderID()
	if err != nil {
		return "", err
	}

	if _, err := a.db.ExecContext(ctx,
		"INSERT INTO orders(id, auction_id, period, type, blocks_json, client_id, created_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
		orderID,
		req.IDAuction,
		req.Period,
		req.Type,
		string(blocksJSON),
		actorID,
		time.Now().UTC().Format(time.RFC3339Nano),
	); err != nil {
		return "", err
	}

	return orderID, nil
}

func (a *App) getOrdersByAuction(ctx context.Context, auctionID string) ([]Order, error) {
	auctionID = strings.TrimSpace(auctionID)
	if auctionID == "" {
		return nil, fmt.Errorf("auction_id is required")
	}

	auction, err := a.getAuction(ctx, auctionID)
	if err != nil {
		return nil, err
	}
	if auction.Status != statusClosed {
		return nil, fmt.Errorf("auction %s is not CLOSED", auctionID)
	}

	rows, err := a.db.QueryContext(ctx, "SELECT id, auction_id, period, type, blocks_json, client_id FROM orders WHERE auction_id = ? ORDER BY period ASC, id ASC", auctionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orders := make([]Order, 0)
	for rows.Next() {
		var order Order
		var blocksJSON string
		if err := rows.Scan(&order.IDOrder, &order.IDAuction, &order.Period, &order.Type, &blocksJSON, &order.ClientID); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(blocksJSON), &order.Blocks); err != nil {
			return nil, fmt.Errorf("invalid blocks_json for order %s: %w", order.IDOrder, err)
		}
		orders = append(orders, order)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return orders, nil
}

func (a *App) addVerity(ctx context.Context, orderID, hash string) error {
	orderID = strings.TrimSpace(orderID)
	hash = strings.TrimSpace(hash)
	if orderID == "" {
		return fmt.Errorf("order_id is required")
	}
	if hash == "" {
		return fmt.Errorf("hash is required")
	}

	_, err := a.db.ExecContext(ctx, "INSERT INTO verity(order_id, hash, created_at) VALUES(?, ?, ?)", orderID, hash, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return fmt.Errorf("verity entry for order_id %s already exists", orderID)
		}
		return err
	}
	return nil
}

func (a *App) updateSettlement(ctx context.Context, marketID string, incoming Settlement) error {
	marketID = strings.TrimSpace(marketID)
	if marketID == "" {
		return fmt.Errorf("market_id is required")
	}
	if err := validateSettlement(incoming); err != nil {
		return fmt.Errorf("invalid settlement payload: %w", err)
	}

	if _, err := a.getMarket(ctx, marketID); err != nil {
		return fmt.Errorf("market %s not found: %w", marketID, err)
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for hour, orders := range incoming.Orders {
		ordersJSON, err := json.Marshal(orders)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM settlements WHERE market_id = ? AND hour = ?", marketID, hour); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO settlements(market_id, hour, orders_json, updated_at) VALUES(?, ?, ?, ?)", marketID, hour, string(ordersJSON), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (a *App) getSettlement(ctx context.Context, marketID string) (*Settlement, error) {
	marketID = strings.TrimSpace(marketID)
	if marketID == "" {
		return nil, fmt.Errorf("market_id is required")
	}

	if _, err := a.getMarket(ctx, marketID); err != nil {
		return nil, err
	}

	rows, err := a.db.QueryContext(ctx, "SELECT hour, orders_json FROM settlements WHERE market_id = ?", marketID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &Settlement{Orders: map[string][]Order{}}
	for rows.Next() {
		var hour string
		var ordersJSON string
		if err := rows.Scan(&hour, &ordersJSON); err != nil {
			return nil, err
		}
		var orders []Order
		if err := json.Unmarshal([]byte(ordersJSON), &orders); err != nil {
			return nil, fmt.Errorf("invalid settlement row for hour %s: %w", hour, err)
		}
		result.Orders[hour] = orders
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (a *App) getEvents(ctx context.Context, afterID int64, eventType string, limit int) ([]event, error) {
	if limit <= 0 {
		limit = 100
	}

	query := "SELECT id, event_type, payload, created_at FROM events WHERE id > ?"
	args := []any{afterID}
	if eventType != "" {
		query += " AND event_type = ?"
		args = append(args, eventType)
	}
	query += " ORDER BY id ASC LIMIT ?"
	args = append(args, limit)

	rows, err := a.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []event
	for rows.Next() {
		var item event
		var payload string
		if err := rows.Scan(&item.ID, &item.Type, &payload, &item.CreatedAt); err != nil {
			return nil, err
		}
		item.Payload = json.RawMessage(payload)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (a *App) getMarket(ctx context.Context, marketID string) (*Market, error) {
	marketID = strings.TrimSpace(marketID)
	if marketID == "" {
		return nil, fmt.Errorf("market_id is required")
	}

	var market Market
	err := a.db.QueryRowContext(ctx, "SELECT id, status FROM markets WHERE id = ?", marketID).Scan(&market.ID, &market.Status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("market %s does not exist", marketID)
		}
		return nil, err
	}

	rows, err := a.db.QueryContext(ctx, "SELECT id FROM auctions WHERE market_id = ? ORDER BY id ASC", marketID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var auctionID string
		if err := rows.Scan(&auctionID); err != nil {
			return nil, err
		}
		market.Auctions = append(market.Auctions, auctionID)
	}

	return &market, rows.Err()
}

func (a *App) getAuction(ctx context.Context, auctionID string) (*Auction, error) {
	auctionID = strings.TrimSpace(auctionID)
	if auctionID == "" {
		return nil, fmt.Errorf("auction_id is required")
	}

	var auction Auction
	err := a.db.QueryRowContext(ctx, "SELECT id, market_id, status, period_begin, period_end FROM auctions WHERE id = ?", auctionID).
		Scan(&auction.ID, &auction.IDMarket, &auction.Status, &auction.PeriodBegin, &auction.PeriodEnd)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("auction %s does not exist", auctionID)
		}
		return nil, err
	}
	return &auction, nil
}

func initSchema(db *sql.DB) error {
	statements := []string{
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA busy_timeout = 10000;`,
		`PRAGMA synchronous = NORMAL;`,
		`CREATE TABLE IF NOT EXISTS markets (
			id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS auctions (
			id TEXT PRIMARY KEY,
			market_id TEXT NOT NULL,
			status TEXT NOT NULL,
			period_begin INTEGER NOT NULL,
			period_end INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			FOREIGN KEY(market_id) REFERENCES markets(id)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_auctions_market ON auctions(market_id);`,
		`CREATE TABLE IF NOT EXISTS orders (
			id TEXT PRIMARY KEY,
			auction_id TEXT NOT NULL,
			period INTEGER NOT NULL,
			type TEXT NOT NULL,
			blocks_json TEXT NOT NULL,
			client_id TEXT NOT NULL,
			created_at TEXT NOT NULL,
			FOREIGN KEY(auction_id) REFERENCES auctions(id)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_orders_auction_period ON orders(auction_id, period);`,
		`CREATE TABLE IF NOT EXISTS verity (
			order_id TEXT PRIMARY KEY,
			hash TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS settlements (
			market_id TEXT NOT NULL,
			hour TEXT NOT NULL,
			orders_json TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (market_id, hour),
			FOREIGN KEY(market_id) REFERENCES markets(id)
		);`,
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_type TEXT NOT NULL,
			payload TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_events_type_id ON events(event_type, id);`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func requireAuctioneer(w http.ResponseWriter, r *http.Request) bool {
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Actor-Role")), "auctioneer") {
		return true
	}
	writeError(w, http.StatusForbidden, "access denied: endpoint allowed only for role auctioneer")
	return false
}

func validateBlocks(blocks []Block) error {
	if len(blocks) == 0 {
		return fmt.Errorf("blocks must not be empty")
	}
	for idx, block := range blocks {
		if block.Amount < 0 {
			return fmt.Errorf("blocks[%d].amount must be >= 0", idx)
		}
	}
	return nil
}

func validateSettlement(s Settlement) error {
	if s.Orders == nil {
		return nil
	}

	hours := make([]string, 0, len(s.Orders))
	for hour := range s.Orders {
		hours = append(hours, hour)
	}
	sort.Strings(hours)

	for _, hour := range hours {
		h, err := strconv.Atoi(hour)
		if err != nil {
			return fmt.Errorf("invalid hour key %q: must be a number between 1 and 24", hour)
		}
		if h < 1 || h > 24 {
			return fmt.Errorf("invalid hour key %q: must be between 1 and 24", hour)
		}

		orders := s.Orders[hour]
		for i, order := range orders {
			if strings.TrimSpace(order.IDOrder) == "" {
				return fmt.Errorf("orders[%s][%d].id_order is required", hour, i)
			}
			if strings.TrimSpace(order.IDAuction) == "" {
				return fmt.Errorf("orders[%s][%d].id_auction is required", hour, i)
			}
			if order.Period < 1 || order.Period > 24 {
				return fmt.Errorf("orders[%s][%d].period must be between 1 and 24", hour, i)
			}
			typeNormalized := strings.ToUpper(strings.TrimSpace(order.Type))
			if typeNormalized != orderTypeBuy && typeNormalized != orderTypeSell {
				return fmt.Errorf("orders[%s][%d].type must be BUY or SELL", hour, i)
			}
		}
	}

	return nil
}

func newOrderID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func writeDomainError(w http.ResponseWriter, err error) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "does not exist"), strings.Contains(msg, "not found"):
		writeError(w, http.StatusNotFound, msg)
	case strings.Contains(msg, "access denied"):
		writeError(w, http.StatusForbidden, msg)
	case strings.Contains(msg, "already exists"), strings.Contains(msg, "must be"), strings.Contains(msg, "is already"), strings.Contains(msg, "is not"), strings.Contains(msg, "required"), strings.Contains(msg, "invalid"), strings.Contains(msg, "outside"):
		writeError(w, http.StatusBadRequest, msg)
	default:
		writeError(w, http.StatusInternalServerError, msg)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
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
