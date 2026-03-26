package main

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
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
}

type FabricClient struct {
	gateway  *client.Gateway
	grpcConn *grpc.ClientConn
	contract *client.Contract
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
	Amount float64 `json:"amount"`
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

type OrderDigestPayload struct {
	IDAuction string        `json:"id_auction"`
	Period    int           `json:"period"`
	Type      string        `json:"type"`
	Blocks    []DigestBlock `json:"blocks"`
}

type DigestBlock struct {
	Amount float64 `json:"amount"`
	Price  float64 `json:"price"`
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

	fabricClient, err := newFabricClient(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to Fabric gateway: %v\n", err)
		os.Exit(1)
	}
	defer fabricClient.Close()

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "list":
		if err := runListCommand(fabricClient, cfg, args); err != nil {
			fmt.Fprintf(os.Stderr, "list failed: %v\n", err)
			os.Exit(1)
		}
	case "create-order":
		if err := runCreateOrderCommand(fabricClient, cfg, args); err != nil {
			fmt.Fprintf(os.Stderr, "create-order failed: %v\n", err)
			os.Exit(1)
		}
	case "settlement":
		if err := runSettlementCommand(fabricClient, cfg, args); err != nil {
			fmt.Fprintf(os.Stderr, "settlement failed: %v\n", err)
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
	fmt.Println(`Fabric market CLI

Usage:
  go run main.go list
  go run main.go create-order --auction <auction-id> --period <1-24> --type BUY|SELL --blocks '[{"amount":10,"price":15.5}]'
  go run main.go create-order --auction <auction-id> --period <1-24> --type BUY|SELL --blocks-file blocks.json
  (create-order computes a SHA-256 hash of auction, period, type, and blocks and submits it with the order)
  go run main.go settlement --auction <auction-id>

Environment variables / defaults:
  FABRIC_CHANNEL_NAME            (default: mychannel)
  FABRIC_CHAINCODE_NAME          (default: market)
  FABRIC_MSP_ID                  (default: Org2MSP)
  FABRIC_PEER_ENDPOINT           (default: dns:///localhost:9051)
  FABRIC_PEER_TLS_HOST_OVERRIDE  (default: peer0.org2.example.com)
  FABRIC_CLIENT_CERT_PATH        (default: Org2 User1 cert path from fabric-samples test-network)
  FABRIC_CLIENT_KEY_PATH         (default: Org2 User1 key path from fabric-samples test-network)
  FABRIC_PEER_TLS_ROOTCERT_PATH  (default: Org2 peer TLS CA path from fabric-samples test-network)
`)
}

func runListCommand(fabricClient *FabricClient, cfg Config, args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.EvaluateTimeout)
	defer cancel()

	market, err := fabricClient.GetOpenedMarket(ctx)
	if err != nil {
		if isNoOpenMarketError(err) {
			printNoOpenMarket()
			return nil
		}
		return err
	}

	var currentAuction *Auction
	currentAuction, err = fabricClient.GetCurrentAuction(ctx)
	if err != nil && !isNoOpenAuctionError(err) {
		return err
	}
	printOpenMarket(market)
	fmt.Println()
	printOpenAuctions(currentAuction)

	return nil
}

func runCreateOrderCommand(fabricClient *FabricClient, cfg Config, args []string) error {
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

	payload, blocks, err := loadBlocksPayload(strings.TrimSpace(*blocksJSON), strings.TrimSpace(*blocksFile))
	if err != nil {
		return err
	}

	hash, err := computeOrderDigest(*auctionID, *period, normalizedType, blocks)
	if err != nil {
		return fmt.Errorf("compute order hash: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.SubmitTimeout+cfg.CommitStatusTimeout)
	defer cancel()

	orderID, err := fabricClient.CreateOrder(ctx, *auctionID, *period, normalizedType, payload, hash)
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
	fmt.Fprintln(w, "Hash (SHA-256)\t"+hash)
	_ = w.Flush()

	fmt.Println()
	printSubmittedBlocks(payload)

	return nil
}

func runSettlementCommand(fabricClient *FabricClient, cfg Config, args []string) error {
	fs := flag.NewFlagSet("settlement", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	auctionID := fs.String("auction", "", "auction ID (e.g. 01-01-2001-A1)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*auctionID) == "" {
		return fmt.Errorf("--auction is required")
	}

	marketID, err := marketIDFromAuction(*auctionID)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.EvaluateTimeout)
	defer cancel()

	settlement, err := fabricClient.GetSettlement(ctx, marketID)
	if err != nil {
		return err
	}

	printSettlement(*auctionID, marketID, settlement)
	return nil
}

func loadBlocksPayload(inlineJSON, filePath string) (string, []Block, error) {
	if inlineJSON != "" && filePath != "" {
		return "", nil, fmt.Errorf("use either --blocks or --blocks-file, not both")
	}

	var raw []byte
	switch {
	case inlineJSON != "":
		raw = []byte(inlineJSON)
	case filePath != "":
		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			return "", nil, fmt.Errorf("read blocks file: %w", err)
		}
		raw = fileBytes
	default:
		return "", nil, fmt.Errorf("one of --blocks or --blocks-file is required")
	}

	var blocks []Block
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return "", nil, fmt.Errorf("invalid blocks JSON: %w", err)
	}
	if len(blocks) == 0 {
		return "", nil, fmt.Errorf("blocks must not be empty")
	}
	for i, block := range blocks {
		if block.Amount <= 0 {
			return "", nil, fmt.Errorf("blocks[%d].amount must be > 0", i)
		}
		if block.Price < 0 {
			return "", nil, fmt.Errorf("blocks[%d].price must be >= 0", i)
		}
	}

	normalized, err := json.Marshal(blocks)
	if err != nil {
		return "", nil, fmt.Errorf("marshal blocks JSON: %w", err)
	}

	return string(normalized), blocks, nil
}

func computeOrderDigest(idAuction string, period int, orderType string, blocks []Block) (string, error) {
	digestBlocks := make([]DigestBlock, len(blocks))
	for i, b := range blocks {
		digestBlocks[i] = DigestBlock{
			Amount: b.Amount,
			Price:  b.Price,
		}
	}

	payload := OrderDigestPayload{
		IDAuction: idAuction,
		Period:    period,
		Type:      orderType,
		Blocks:    digestBlocks,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	return sha256Hex(b), nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
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

func printSubmittedBlocks(blocksJSON string) {
	var blocks []Block
	if err := json.Unmarshal([]byte(blocksJSON), &blocks); err != nil {
		fmt.Printf("Submitted blocks JSON: %s\n", blocksJSON)
		return
	}

	fmt.Println("BLOCKS")
	fmt.Println("======")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "#\tAMOUNT\tPRICE")
	for i, block := range blocks {
		fmt.Fprintf(w, "%d\t%.6f\t%.6f\n", i+1, block.Amount, block.Price)
	}
	_ = w.Flush()
}

func printSettlement(auctionID, marketID string, settlement *Settlement) {
	fmt.Println("SETTLEMENT")
	fmt.Println("==========")
	fmt.Printf("Market: %s\n", marketID)
	fmt.Printf("Auction: %s\n", auctionID)

	if settlement == nil || len(settlement.Orders) == 0 {
		fmt.Println("No settlement found.")
		return
	}

	hours := make([]int, 0, len(settlement.Orders))
	for h := range settlement.Orders {
		if v, err := strconv.Atoi(h); err == nil {
			hours = append(hours, v)
		}
	}
	sort.Ints(hours)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "HOUR\tORDER ID\tTYPE\tCLIENT ID\tPERIOD\tBLOCKS\tTOTAL AMOUNT")

	var matched bool
	for _, h := range hours {
		hourStr := strconv.Itoa(h)
		for _, order := range settlement.Orders[hourStr] {
			if order.IDAuction != auctionID {
				continue
			}
			matched = true
			totalAmount := sumBlockAmounts(order.Blocks)
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%.6f\n",
				hourStr,
				order.IDOrder,
				order.Type,
				order.ClientID,
				order.Period,
				len(order.Blocks),
				totalAmount,
			)
		}
	}
	_ = w.Flush()

	if !matched {
		fmt.Println("No settlement entries for this auction.")
	}
}

func sumBlockAmounts(blocks []Block) float64 {
	var total float64
	for _, b := range blocks {
		total += b.Amount
	}
	return total
}

func marketIDFromAuction(auctionID string) (string, error) {
	auctionID = strings.TrimSpace(auctionID)
	if auctionID == "" {
		return "", fmt.Errorf("auction ID is required")
	}

	parts := strings.Split(auctionID, "-")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid auction ID format %q", auctionID)
	}

	marketID := strings.Join(parts[:len(parts)-1], "-")
	if marketID == "" {
		return "", fmt.Errorf("could not derive market ID from auction ID %q", auctionID)
	}
	return marketID, nil
}

func loadConfig() (Config, error) {
	cfg := Config{
		ChannelName:         envOrDefault("FABRIC_CHANNEL_NAME", "mychannel"),
		ChaincodeName:       envOrDefault("FABRIC_CHAINCODE_NAME", "market"),
		MSPID:               envOrDefault("FABRIC_MSP_ID", "Org2MSP"),
		PeerEndpoint:        strings.TrimSpace(envOrDefault("FABRIC_PEER_ENDPOINT", "dns:///localhost:9051")),
		PeerTLSHostOverride: strings.TrimSpace(envOrDefault("FABRIC_PEER_TLS_HOST_OVERRIDE", "peer0.org2.example.com")),
		ClientCertPath:      strings.TrimSpace(envOrDefault("FABRIC_CLIENT_CERT_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org2.example.com/users/User1@org2.example.com/msp/signcerts")),
		ClientKeyPath:       strings.TrimSpace(envOrDefault("FABRIC_CLIENT_KEY_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org2.example.com/users/User1@org2.example.com/msp/keystore")),
		PeerTLSRootCertPath: strings.TrimSpace(envOrDefault("FABRIC_PEER_TLS_ROOTCERT_PATH", "../../fabric-samples/test-network/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt")),
	}

	var err error
	cfg.EvaluateTimeout, err = parseDurationEnv("FABRIC_EVALUATE_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.EndorseTimeout, err = parseDurationEnv("FABRIC_ENDORSE_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.SubmitTimeout, err = parseDurationEnv("FABRIC_SUBMIT_TIMEOUT", 15*time.Second)
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

func newFabricClient(cfg Config) (*FabricClient, error) {
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

	return &FabricClient{
		gateway:  gateway,
		grpcConn: grpcConn,
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

func (c *FabricClient) GetOpenedMarket(ctx context.Context) (*Market, error) {
	result, err := c.contract.EvaluateWithContext(ctx, "GetOpenedMarket")
	if err != nil {
		return nil, wrapGatewayError("GetOpenedMarket", err)
	}

	var market Market
	if err := json.Unmarshal(result, &market); err != nil {
		return nil, fmt.Errorf("decode GetOpenedMarket response: %w", err)
	}

	return &market, nil
}

func (c *FabricClient) GetCurrentAuction(ctx context.Context) (*Auction, error) {
	result, err := c.contract.EvaluateWithContext(ctx, "GetCurrentAuction")
	if err != nil {
		return nil, wrapGatewayError("GetCurrentAuction", err)
	}

	var auction Auction
	if err := json.Unmarshal(result, &auction); err != nil {
		return nil, fmt.Errorf("decode GetCurrentAuction response: %w", err)
	}

	return &auction, nil
}

func (c *FabricClient) GetSettlement(ctx context.Context, marketID string) (*Settlement, error) {
	result, err := c.contract.EvaluateWithContext(ctx, "GetSettlement", client.WithArguments(marketID))
	if err != nil {
		return nil, wrapGatewayError("GetSettlement", err)
	}

	var settlement Settlement
	if err := json.Unmarshal(result, &settlement); err != nil {
		return nil, fmt.Errorf("decode GetSettlement response: %w", err)
	}
	return &settlement, nil
}

func (c *FabricClient) CreateOrder(ctx context.Context, auctionID string, period int, orderType string, blocksJSON string, providedHash string) (string, error) {
	result, err := c.contract.SubmitWithContext(
		ctx,
		"AddOrder",
		client.WithArguments(auctionID, strconv.Itoa(period), orderType, "", providedHash),
		client.WithTransient(map[string][]byte{
			"blocks": []byte(blocksJSON),
		}),
	)
	if err != nil {
		return "", wrapGatewayError("AddOrder", err)
	}

	var orderID string
	if err := json.Unmarshal(result, &orderID); err == nil {
		return orderID, nil
	}

	return strings.TrimSpace(string(result)), nil
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

func isNoOpenMarketError(err error) bool {
	return strings.Contains(err.Error(), "no OPEN market found")
}

func isNoOpenAuctionError(err error) bool {
	return strings.Contains(err.Error(), "no OPEN auction found")
}
