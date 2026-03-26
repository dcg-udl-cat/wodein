package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

type Config struct {
	APIBaseURL     string
	ActorID        string
	RequestTimeout time.Duration
}

type APIClient struct {
	baseURL string
	actorID string
	http    *http.Client
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

type addOrderRequest struct {
	IDAuction string  `json:"id_auction"`
	Period    int     `json:"period"`
	Type      string  `json:"type"`
	Blocks    []Block `json:"blocks"`
}

type addOrderResponse struct {
	OrderID string `json:"order_id"`
}

type apiError struct {
	Error string `json:"error"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	apiClient := &APIClient{
		baseURL: strings.TrimRight(cfg.APIBaseURL, "/"),
		actorID: cfg.ActorID,
		http:    &http.Client{Timeout: cfg.RequestTimeout},
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "list":
		if err := runListCommand(apiClient, cfg, args); err != nil {
			fmt.Fprintf(os.Stderr, "list failed: %v\n", err)
			os.Exit(1)
		}
	case "create-order":
		if err := runCreateOrderCommand(apiClient, cfg, args); err != nil {
			fmt.Fprintf(os.Stderr, "create-order failed: %v\n", err)
			os.Exit(1)
		}
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`HTTP market CLI

Usage:
  go run main.go list
  go run main.go create-order --auction <auction-id> --period <1-24> --type BUY|SELL --blocks '[{"amount":10,"price":15.5}]'
  go run main.go create-order --auction <auction-id> --period <1-24> --type BUY|SELL --blocks-file blocks.json

Environment variables / defaults:
  APP_API_BASE_URL    (default: http://127.0.0.1:8080)
  APP_ACTOR_ID        (default: client-1)
  APP_REQUEST_TIMEOUT (default: 15s)
`)
}

func runListCommand(apiClient *APIClient, cfg Config, args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.RequestTimeout)
	defer cancel()

	market, err := apiClient.GetOpenedMarket(ctx)
	if err != nil {
		if isNoOpenMarketError(err) {
			printNoOpenMarket()
			return nil
		}
		return err
	}

	currentAuction, err := apiClient.GetCurrentAuction(ctx)
	if err != nil && !isNoOpenAuctionError(err) {
		return err
	}

	printOpenMarket(market)
	fmt.Println()
	printOpenAuctions(currentAuction)

	return nil
}

func runCreateOrderCommand(apiClient *APIClient, cfg Config, args []string) error {
	fs := flag.NewFlagSet("create-order", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	auctionID := fs.String("auction", "", "auction ID")
	period := fs.Int("period", 0, "period number (1-24)")
	orderType := fs.String("type", "BUY", "order type: BUY or SELL")
	blocksJSON := fs.String("blocks", "", "JSON array of blocks, example: [{\"amount\":10,\"price\":15.5}]")
	blocksFile := fs.String("blocks-file", "", "path to a JSON file containing the blocks array")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*auctionID) == "" {
		return fmt.Errorf("--auction is required")
	}
	if *period < 1 || *period > 24 {
		return fmt.Errorf("--period must be between 1 and 24")
	}

	normalizedType := strings.ToUpper(strings.TrimSpace(*orderType))
	if normalizedType != "BUY" && normalizedType != "SELL" {
		return fmt.Errorf("--type must be BUY or SELL")
	}

	blocks, err := loadBlocks(strings.TrimSpace(*blocksJSON), strings.TrimSpace(*blocksFile))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.RequestTimeout)
	defer cancel()

	orderID, err := apiClient.CreateOrder(ctx, addOrderRequest{
		IDAuction: *auctionID,
		Period:    *period,
		Type:      normalizedType,
		Blocks:    blocks,
	})
	if err != nil {
		return err
	}

	fmt.Println("ORDER CREATED")
	fmt.Println("=============")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "FIELD\tVALUE")
	fmt.Fprintln(w, "Order ID\t"+orderID)
	fmt.Fprintln(w, "Auction ID\t"+*auctionID)
	fmt.Fprintln(w, "Period\t"+strconv.Itoa(*period))
	fmt.Fprintln(w, "Type\t"+normalizedType)
	_ = w.Flush()

	fmt.Println()
	printSubmittedBlocks(blocks)

	return nil
}

func loadBlocks(inlineJSON, filePath string) ([]Block, error) {
	if inlineJSON != "" && filePath != "" {
		return nil, fmt.Errorf("use either --blocks or --blocks-file, not both")
	}

	var raw []byte
	switch {
	case inlineJSON != "":
		raw = []byte(inlineJSON)
	case filePath != "":
		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("read blocks file: %w", err)
		}
		raw = fileBytes
	default:
		return nil, fmt.Errorf("one of --blocks or --blocks-file is required")
	}

	var blocks []Block
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, fmt.Errorf("invalid blocks JSON: %w", err)
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("blocks must not be empty")
	}
	for i, block := range blocks {
		if block.Amount < 0 {
			return nil, fmt.Errorf("blocks[%d].amount must be >= 0", i)
		}
	}

	return blocks, nil
}

func printNoOpenMarket() {
	fmt.Println("OPEN MARKET")
	fmt.Println("===========")
	fmt.Println("No OPEN market found.")
	fmt.Println()
	fmt.Println("OPEN AUCTIONS")
	fmt.Println("=============")
	fmt.Println("No OPEN auction found.")
}

func printOpenMarket(market *Market) {
	fmt.Println("OPEN MARKET")
	fmt.Println("===========")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "MARKET ID\tSTATUS\tAUCTION IDS")
	fmt.Fprintf(w, "%s\t%s\t%s\n", market.ID, market.Status, strings.Join(market.Auctions, ", "))
	_ = w.Flush()
}

func printOpenAuctions(auction *Auction) {
	fmt.Println("OPEN AUCTIONS")
	fmt.Println("=============")

	if auction == nil {
		fmt.Println("No OPEN auction found in the OPEN market.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "AUCTION ID\tMARKET ID\tSTATUS\tPERIOD BEGIN\tPERIOD END")
	fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\n", auction.ID, auction.IDMarket, auction.Status, auction.PeriodBegin, auction.PeriodEnd)
	_ = w.Flush()
}

func printSubmittedBlocks(blocks []Block) {
	fmt.Println("BLOCKS")
	fmt.Println("======")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "#\tAMOUNT\tPRICE")
	for i, block := range blocks {
		fmt.Fprintf(w, "%d\t%d\t%.6f\n", i+1, block.Amount, block.Price)
	}
	_ = w.Flush()
}

func loadConfig() (Config, error) {
	cfg := Config{
		APIBaseURL: envOrDefault("APP_API_BASE_URL", "http://127.0.0.1:8080"),
		ActorID:    envOrDefault("APP_ACTOR_ID", "client-1"),
	}

	var err error
	cfg.RequestTimeout, err = parseDurationEnv("APP_REQUEST_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c *APIClient) GetOpenedMarket(ctx context.Context) (*Market, error) {
	var market Market
	if err := c.getJSON(ctx, "/markets/open", &market); err != nil {
		return nil, err
	}
	return &market, nil
}

func (c *APIClient) GetCurrentAuction(ctx context.Context) (*Auction, error) {
	var auction Auction
	if err := c.getJSON(ctx, "/auctions/current", &auction); err != nil {
		return nil, err
	}
	return &auction, nil
}

func (c *APIClient) CreateOrder(ctx context.Context, req addOrderRequest) (string, error) {
	var response addOrderResponse
	if err := c.postJSON(ctx, "/orders", req, &response); err != nil {
		return "", err
	}
	return strings.TrimSpace(response.OrderID), nil
}

func (c *APIClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Actor-Role", "client")
	req.Header.Set("X-Actor-ID", c.actorID)

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
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Actor-Role", "client")
	req.Header.Set("X-Actor-ID", c.actorID)

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

func envOrDefault(name, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return defaultValue
	}
	return value
}

func isNoOpenMarketError(err error) bool {
	return strings.Contains(err.Error(), "no OPEN market found")
}

func isNoOpenAuctionError(err error) bool {
	return strings.Contains(err.Error(), "no OPEN auction found")
}
