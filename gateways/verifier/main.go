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
	"strings"
	"text/tabwriter"
	"time"

	"github.com/hyperledger/fabric-gateway/pkg/client"
	gatewayhash "github.com/hyperledger/fabric-gateway/pkg/hash"
	"github.com/hyperledger/fabric-gateway/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Config holds gateway connection configuration.
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

// FabricVerifier wraps gateway client and contract handle.
type FabricVerifier struct {
	gateway  *client.Gateway
	grpcConn *grpc.ClientConn
	contract *client.Contract
}

// Domain models copied from chaincode JSON schema.
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

type OrderHashRecord struct {
	OrderID   string `json:"order_id"`
	Hash      string `json:"hash"`
	Algorithm string `json:"algorithm"`
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

	verifier, err := newFabricVerifier(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to Fabric gateway: %v\n", err)
		os.Exit(1)
	}
	defer verifier.Close()

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "verify":
		if err := runVerifyCommand(verifier, cfg, args); err != nil {
			fmt.Fprintf(os.Stderr, "verify failed: %v\n", err)
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
	fmt.Println(`Verity verifier CLI

Usage:
  go run main.go verify --market <market-id> [--auction <auction-id>]
  (if --auction is omitted, all orders in the market settlement are verified)

Environment variables / defaults:
  FABRIC_CHANNEL_NAME            (default: mychannel)
  FABRIC_CHAINCODE_NAME          (default: market)
  FABRIC_MSP_ID                  (default: Org1MSP)
  FABRIC_PEER_ENDPOINT           (default: dns:///localhost:7051)
  FABRIC_PEER_TLS_HOST_OVERRIDE  (default: peer0.org1.example.com)
  FABRIC_CLIENT_CERT_PATH        (default: Org1 User1 cert path from fabric-samples test-network)
  FABRIC_CLIENT_KEY_PATH         (default: Org1 User1 key path from fabric-samples test-network)
  FABRIC_PEER_TLS_ROOTCERT_PATH  (default: Org1 peer TLS CA path from fabric-samples test-network)
`)
}

func runVerifyCommand(verifier *FabricVerifier, cfg Config, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	marketIDFlag := fs.String("market", "", "market ID (e.g. 01-01-2001)")
	auctionID := fs.String("auction", "", "optional auction ID filter (e.g. 01-01-2001-A1)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	var marketID string
	if strings.TrimSpace(*marketIDFlag) != "" {
		marketID = strings.TrimSpace(*marketIDFlag)
	}
	if strings.TrimSpace(*auctionID) != "" && marketID == "" {
		var err error
		marketID, err = marketIDFromAuction(*auctionID)
		if err != nil {
			return err
		}
	}
	if marketID == "" {
		return fmt.Errorf("one of --market or --auction is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.EvaluateTimeout)
	defer cancel()

	settlement, err := verifier.GetSettlement(ctx, marketID)
	if err != nil {
		return err
	}

	report := verifier.VerifySettlement(ctx, strings.TrimSpace(*auctionID), settlement)
	printReport(marketID, strings.TrimSpace(*auctionID), report)
	if len(report.Failures) > 0 {
		return fmt.Errorf("%d verification failures", len(report.Failures))
	}

	return nil
}

// VerificationReport aggregates results for user-friendly output.
type VerificationReport struct {
	Checked  int
	Matches  int
	Failures []string
	Rows     []ReportRow
}

type ReportRow struct {
	OrderID        string
	ClientID       string
	Result         string
	ComputedHash   string
	OnChainHash    string
	Algorithm      string
	Reason         string
	Hour           string
	Period         int
	OrderType      string
	BlocksCount    int
	TotalBlockSize float64
}

func (v *FabricVerifier) VerifySettlement(ctx context.Context, auctionFilter string, settlement *Settlement) VerificationReport {
	if settlement == nil || len(settlement.Orders) == 0 {
		return VerificationReport{}
	}
	auctionFilter = strings.TrimSpace(auctionFilter)

	hours := make([]string, 0, len(settlement.Orders))
	for h := range settlement.Orders {
		hours = append(hours, h)
	}
	sort.Strings(hours)

	// Load original orders from the ledger so we hash the exact payload that
	// produced the on-chain verity entry (settlement orders may be trimmed by
	// the oracle).
	originalOrders := make(map[string]Order)
	auctionsToFetch := make(map[string]struct{})
	for _, hour := range hours {
		for _, order := range settlement.Orders[hour] {
			if auctionFilter != "" && order.IDAuction != auctionFilter {
				continue
			}
			auctionsToFetch[order.IDAuction] = struct{}{}
		}
	}

	for auctionID := range auctionsToFetch {
		orders, err := v.GetOrdersByAuction(ctx, auctionID)
		if err != nil {
			continue
		}
		for _, o := range orders {
			originalOrders[o.IDOrder] = o
		}
	}

	var report VerificationReport

	for _, hour := range hours {
		for _, order := range settlement.Orders[hour] {
			if auctionFilter != "" && order.IDAuction != auctionFilter {
				continue
			}

			canonical := order
			if orig, ok := originalOrders[order.IDOrder]; ok && orig.IDOrder != "" {
				canonical = orig
			}

			row := ReportRow{
				OrderID:        order.IDOrder,
				ClientID:       canonical.ClientID,
				Hour:           hour,
				Period:         canonical.Period,
				OrderType:      canonical.Type,
				BlocksCount:    len(canonical.Blocks),
				TotalBlockSize: sumBlockAmounts(canonical.Blocks),
			}
			report.Checked++

			computed, err := computeOrderDigest(canonical.IDAuction, canonical.Period, canonical.Type, canonical.Blocks)
			if err != nil {
				row.Result = "ERROR"
				row.Reason = fmt.Sprintf("compute hash: %v", err)
				report.Failures = append(report.Failures, row.OrderID)
				report.Rows = append(report.Rows, row)
				continue
			}
			row.ComputedHash = computed

			rec, err := v.GetVerity(ctx, order.IDOrder)
			if err != nil {
				row.Result = "MISSING"
				row.Reason = err.Error()
				report.Failures = append(report.Failures, row.OrderID)
				report.Rows = append(report.Rows, row)
				continue
			}

			row.OnChainHash = strings.ToLower(strings.TrimSpace(rec.Hash))
			row.Algorithm = rec.Algorithm

			if row.OnChainHash == row.ComputedHash {
				row.Result = "OK"
				report.Matches++
			} else {
				fmt.Println(row)
				row.Result = "MISMATCH"
				row.Reason = "computed != on-chain"
				report.Failures = append(report.Failures, row.OrderID)
			}

			report.Rows = append(report.Rows, row)
		}
	}

	return report
}

func printReport(marketID, auctionFilter string, report VerificationReport) {
	fmt.Println("VERITY VERIFICATION")
	fmt.Println("===================")
	fmt.Printf("Market: %s\n", marketID)
	if strings.TrimSpace(auctionFilter) == "" {
		fmt.Println("Auction: ALL")
	} else {
		fmt.Printf("Auction: %s\n", auctionFilter)
	}
	fmt.Printf("Checked orders: %d | Matches: %d | Failures: %d\n", report.Checked, report.Matches, len(report.Failures))

	if len(report.Rows) == 0 {
		fmt.Println("No settlement orders to verify.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "HOUR\tORDER ID\tTYPE\tCLIENT ID\tRESULT\tHASH\tON-CHAIN HASH\tALG\tBLOCKS\tTOTAL AMOUNT")
	for _, row := range report.Rows {
		hash := row.ComputedHash
		onChain := row.OnChainHash
		if hash == "" {
			hash = "-"
		}
		if onChain == "" {
			onChain = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%.6f\n",
			row.Hour,
			row.OrderID,
			row.OrderType,
			row.ClientID,
			row.Result,
			hash,
			onChain,
			row.Algorithm,
			row.BlocksCount,
			row.TotalBlockSize,
		)
	}
	_ = w.Flush()

	if len(report.Failures) > 0 {
		fmt.Println()
		fmt.Printf("FAILURES: %s\n", strings.Join(report.Failures, ", "))
	}
}

func (c *FabricVerifier) Close() {
	if c.gateway != nil {
		c.gateway.Close()
	}
	if c.grpcConn != nil {
		_ = c.grpcConn.Close()
	}
}

func newFabricVerifier(cfg Config) (*FabricVerifier, error) {
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

	return &FabricVerifier{
		gateway:  gateway,
		grpcConn: grpcConn,
		contract: contract,
	}, nil
}

func (c *FabricVerifier) GetSettlement(ctx context.Context, marketID string) (*Settlement, error) {
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

func (c *FabricVerifier) GetVerity(ctx context.Context, orderID string) (*OrderHashRecord, error) {
	result, err := c.contract.EvaluateWithContext(ctx, "GetVerity", client.WithArguments(orderID))
	if err != nil {
		return nil, wrapGatewayError("GetVerity", err)
	}

	var record OrderHashRecord
	if err := json.Unmarshal(result, &record); err != nil {
		return nil, fmt.Errorf("decode GetVerity response: %w", err)
	}
	return &record, nil
}

func (c *FabricVerifier) GetOrdersByAuction(ctx context.Context, auctionID string) ([]Order, error) {
	result, err := c.contract.EvaluateWithContext(ctx, "GetOrdersByAuction", client.WithArguments(auctionID))
	if err != nil {
		return nil, wrapGatewayError("GetOrdersByAuction", err)
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

	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
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

// Config helpers (copied from client CLI for consistency).
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
	if !strings.Contains(target, "://") {
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
