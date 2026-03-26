package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

const (
	auctioneerMSP = "Org1MSP"
	org1MSP       = "Org2MSP"
	org2MSP       = "Org3MSP"

	statusNewlyCreated = "NEWLY_CREATED"
	statusOpen         = "OPEN"
	statusClosed       = "CLOSED"

	orderTypeBuy  = "BUY"
	orderTypeSell = "SELL"

	marketKeyPrefix     = "MARKET_"
	auctionKeyPrefix    = "AUCTION_"
	orderKeyPrefix      = "ORDER_"
	verityKeyPrefix     = "VERITY_"
	verityStateKey      = "VERITY"
	settlementsStateKey = "SETTLEMENTS"

	collectionOrg1Auctioneer = "collectionOrg2Auctioneer"
	collectionOrg2Auctioneer = "collectionOrg3Auctioneer"
)

type SmartContract struct {
	contractapi.Contract
}

type Auction struct {
	ID          string `json:"id"`
	IDMarket    string `json:"id_market"`
	Status      string `json:"status"`
	PeriodBegin int    `json:"period_begin"`
	PeriodEnd   int    `json:"period_end"`
}

type Market struct {
	ID       string   `json:"id"`
	Status   string   `json:"status"`
	Auctions []string `json:"auctions"`
}

type Verity struct {
	OrderHashes map[string]string `json:"order_hashes"`
}

type OrderDigestPayload struct {
	IDAuction string        `json:"id_auction"`
	Period    int           `json:"period"`
	Type      string        `json:"type"`
	Blocks    []DigestBlock `json:"blocks"`
}

type OrderHashRecord struct {
	OrderID   string `json:"order_id"`
	Hash      string `json:"hash"`
	Algorithm string `json:"algorithm"`
}

type Block struct {
	Amount float64 `json:"amount"`
	Price  float64 `json:"price"`
	Status string  `json:"status,omitempty"`
}

type DigestBlock struct {
	Amount float64 `json:"amount"`
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

type Settlements map[string]Settlement

type AcceptedOrderEvent struct {
	IDOrder string `json:"id_order"`
}

type CloseAuctionEvent struct {
	IDAuction string `json:"id_auction"`
}

func main() {
	cc, err := contractapi.NewChaincode(&SmartContract{})
	if err != nil {
		panic(fmt.Sprintf("failed to create chaincode: %v", err))
	}

	if err := cc.Start(); err != nil {
		panic(fmt.Sprintf("failed to start chaincode: %v", err))
	}
}

// =========================
// Public Transactions
// =========================

// CreateMarket creates a market for a given date and three auctions:
// - Auction 1: period 1..24
// - Auction 2: period 1..24
// - Auction 3: period 13..24
func (s *SmartContract) CreateMarket(ctx contractapi.TransactionContextInterface, date string) error {
	if err := s.assertAuctioneer(ctx); err != nil {
		return err
	}

	date = strings.TrimSpace(date)
	if date == "" {
		return fmt.Errorf("date is required")
	}

	// Expected format: dd-mm-yyyy
	if _, err := time.Parse("02-01-2006", date); err != nil {
		return fmt.Errorf("invalid date format %q, expected dd-mm-yyyy", date)
	}

	marketKey := buildMarketKey(date)
	exists, err := s.stateExists(ctx, marketKey)
	if err != nil {
		return fmt.Errorf("failed checking market existence: %w", err)
	}
	if exists {
		return fmt.Errorf("market %s already exists", date)
	}

	markets, err := s.GetMarkets(ctx)
	if err != nil {
		return fmt.Errorf("failed loading markets: %w", err)
	}
	for _, m := range markets {
		if m.Status == statusNewlyCreated || m.Status == statusOpen {
			return fmt.Errorf("cannot create market %s: market %s is already %s", date, m.ID, m.Status)
		}
	}

	auction1ID := fmt.Sprintf("%s-A1", date)
	auction2ID := fmt.Sprintf("%s-A2", date)
	auction3ID := fmt.Sprintf("%s-A3", date)

	market := Market{
		ID:       date,
		Status:   statusNewlyCreated,
		Auctions: []string{auction1ID, auction2ID, auction3ID},
	}

	auction1 := Auction{
		ID:          auction1ID,
		IDMarket:    date,
		Status:      statusNewlyCreated,
		PeriodBegin: 1,
		PeriodEnd:   24,
	}
	auction2 := Auction{
		ID:          auction2ID,
		IDMarket:    date,
		Status:      statusNewlyCreated,
		PeriodBegin: 1,
		PeriodEnd:   24,
	}
	auction3 := Auction{
		ID:          auction3ID,
		IDMarket:    date,
		Status:      statusNewlyCreated,
		PeriodBegin: 13,
		PeriodEnd:   24,
	}

	if err := s.putMarket(ctx, &market); err != nil {
		return fmt.Errorf("failed storing market: %w", err)
	}
	if err := s.putAuction(ctx, &auction1); err != nil {
		return fmt.Errorf("failed storing auction1: %w", err)
	}
	if err := s.putAuction(ctx, &auction2); err != nil {
		return fmt.Errorf("failed storing auction2: %w", err)
	}
	if err := s.putAuction(ctx, &auction3); err != nil {
		return fmt.Errorf("failed storing auction3: %w", err)
	}

	return nil
}

func (s *SmartContract) OpenAuction(ctx contractapi.TransactionContextInterface, auctionID string) error {
	if err := s.assertAuctioneer(ctx); err != nil {
		return err
	}

	auction, err := s.getAuction(ctx, auctionID)
	if err != nil {
		return err
	}
	if auction.Status != statusNewlyCreated {
		return fmt.Errorf("auction %s must be %s to open, current status: %s", auctionID, statusNewlyCreated, auction.Status)
	}

	auction.Status = statusOpen
	if err := s.putAuction(ctx, auction); err != nil {
		return fmt.Errorf("failed updating auction %s: %w", auctionID, err)
	}

	market, err := s.getMarket(ctx, auction.IDMarket)
	if err != nil {
		return err
	}
	if market.Status == statusNewlyCreated {
		market.Status = statusOpen
		if err := s.putMarket(ctx, market); err != nil {
			return fmt.Errorf("failed updating market %s: %w", market.ID, err)
		}
	}

	return nil
}

func (s *SmartContract) CloseAuction(ctx contractapi.TransactionContextInterface, auctionID string) error {
	if err := s.assertAuctioneer(ctx); err != nil {
		return err
	}

	auction, err := s.getAuction(ctx, auctionID)
	if err != nil {
		return err
	}

	if auction.Status == statusClosed {
		return fmt.Errorf("auction %s is already CLOSED", auctionID)
	}

	auction.Status = statusClosed
	if err := s.putAuction(ctx, auction); err != nil {
		return fmt.Errorf("failed updating auction %s: %w", auctionID, err)
	}

	// Emit an event so the oracle can start
	eventPayload, err := json.Marshal(CloseAuctionEvent{
		IDAuction: auctionID,
	})
	if err != nil {
		return fmt.Errorf("failed marshalling CloseAuction event: %w", err)
	}

	if err := ctx.GetStub().SetEvent("CloseAuction", eventPayload); err != nil {
		return fmt.Errorf("failed emitting CloseAuction event: %w", err)
	}

	market, err := s.getMarket(ctx, auction.IDMarket)
	if err != nil {
		return err
	}

	allClosed := true
	for _, siblingID := range market.Auctions {
		sibling, err := s.getAuction(ctx, siblingID)
		if err != nil {
			return fmt.Errorf("failed reading sibling auction %s: %w", siblingID, err)
		}
		if sibling.Status != statusClosed {
			allClosed = false
			break
		}
	}

	if allClosed {
		market.Status = statusClosed
		if err := s.putMarket(ctx, market); err != nil {
			return fmt.Errorf("failed updating market %s: %w", market.ID, err)
		}
	}

	return nil
}

func (s *SmartContract) AddVerity(ctx contractapi.TransactionContextInterface, orderID string, hash string) error {
	orderID = strings.TrimSpace(orderID)
	hash = strings.ToLower(strings.TrimSpace(hash))

	if orderID == "" {
		return fmt.Errorf("order_id is required")
	}
	if hash == "" {
		return fmt.Errorf("hash is required")
	}
	if len(hash) != 64 {
		return fmt.Errorf("hash must be a sha256 hex string")
	}

	if existing, err := ctx.GetStub().GetState(buildVerityKey(orderID)); err != nil {
		return fmt.Errorf("failed checking existing verity record: %w", err)
	} else if existing != nil {
		return fmt.Errorf("verity entry for order_id %s already exists", orderID)
	}

	if err := s.saveVerityHash(ctx, orderID, hash); err != nil {
		return fmt.Errorf("failed storing verity: %w", err)
	}

	return nil
}

func (s *SmartContract) GetVerity(ctx contractapi.TransactionContextInterface, orderID string) (*OrderHashRecord, error) {
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return nil, fmt.Errorf("order_id is required")
	}

	b, err := ctx.GetStub().GetState(buildVerityKey(orderID))
	if err != nil {
		return nil, fmt.Errorf("failed reading verity for order %s: %w", orderID, err)
	}
	if b == nil {
		return nil, fmt.Errorf("verity entry for order_id %s not found", orderID)
	}

	var record OrderHashRecord
	if err := json.Unmarshal(b, &record); err != nil {
		return nil, fmt.Errorf("failed unmarshalling verity for order %s: %w", orderID, err)
	}

	return &record, nil
}

// UpdateSettlement deep-merges by hour:
// - existing hour in ledger but not in input => keep existing
// - hour in input => overwrite ledger hour
func (s *SmartContract) UpdateSettlement(ctx contractapi.TransactionContextInterface, marketID string, newSettlementJSON string) error {
	if err := s.assertAuctioneer(ctx); err != nil {
		return err
	}

	marketID = strings.TrimSpace(marketID)
	if marketID == "" {
		return fmt.Errorf("market_id is required")
	}
	if strings.TrimSpace(newSettlementJSON) == "" {
		return fmt.Errorf("newSettlement is required")
	}

	if _, err := s.getMarket(ctx, marketID); err != nil {
		return fmt.Errorf("market %s not found: %w", marketID, err)
	}

	var incoming Settlement
	if err := json.Unmarshal([]byte(newSettlementJSON), &incoming); err != nil {
		return fmt.Errorf("invalid settlement JSON: %w", err)
	}
	if err := validateSettlement(incoming); err != nil {
		return fmt.Errorf("invalid settlement payload: %w", err)
	}

	allSettlements, err := s.getAllSettlements(ctx)
	if err != nil {
		return fmt.Errorf("failed reading settlements: %w", err)
	}

	current, exists := allSettlements[marketID]
	if !exists {
		current = Settlement{Orders: map[string][]Order{}}
	}
	if current.Orders == nil {
		current.Orders = map[string][]Order{}
	}

	merged := Settlement{Orders: map[string][]Order{}}

	// Keep ledger version by default
	for hour, orders := range current.Orders {
		merged.Orders[hour] = orders
	}

	// Overwrite with incoming if present
	for hour, orders := range incoming.Orders {
		merged.Orders[hour] = orders
	}

	allSettlements[marketID] = merged

	if err := s.putAllSettlements(ctx, allSettlements); err != nil {
		return fmt.Errorf("failed storing settlements: %w", err)
	}

	return nil
}

func (s *SmartContract) GetMarkets(ctx contractapi.TransactionContextInterface) ([]Market, error) {
	iter, err := ctx.GetStub().GetStateByRange(rangeStart(marketKeyPrefix), rangeEnd(marketKeyPrefix))
	if err != nil {
		return nil, fmt.Errorf("failed to query markets: %w", err)
	}
	defer iter.Close()

	var markets []Market
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("failed iterating markets: %w", err)
		}

		var m Market
		if err := json.Unmarshal(kv.Value, &m); err != nil {
			return nil, fmt.Errorf("failed unmarshalling market key %s: %w", kv.Key, err)
		}
		markets = append(markets, m)
	}

	sort.Slice(markets, func(i, j int) bool {
		return markets[i].ID < markets[j].ID
	})

	return markets, nil
}

func (s *SmartContract) GetOpenedMarket(ctx contractapi.TransactionContextInterface) (*Market, error) {
	markets, err := s.GetMarkets(ctx)
	if err != nil {
		return nil, err
	}

	for _, m := range markets {
		if m.Status == statusOpen {
			copyMarket := m
			return &copyMarket, nil
		}
	}

	return nil, fmt.Errorf("no OPEN market found")
}

func (s *SmartContract) GetCurrentAuction(ctx contractapi.TransactionContextInterface) (*Auction, error) {
	market, err := s.GetOpenedMarket(ctx)
	if err != nil {
		return nil, err
	}

	var openedAuctions []Auction
	for _, auctionID := range market.Auctions {
		a, err := s.getAuction(ctx, auctionID)
		if err != nil {
			return nil, fmt.Errorf("failed reading auction %s: %w", auctionID, err)
		}
		if a.Status == statusOpen {
			openedAuctions = append(openedAuctions, *a)
		}
	}

	if len(openedAuctions) == 0 {
		return nil, fmt.Errorf("no OPEN auction found in market %s", market.ID)
	}

	sort.Slice(openedAuctions, func(i, j int) bool {
		if openedAuctions[i].PeriodBegin == openedAuctions[j].PeriodBegin {
			return openedAuctions[i].ID < openedAuctions[j].ID
		}
		return openedAuctions[i].PeriodBegin < openedAuctions[j].PeriodBegin
	})

	current := openedAuctions[0]
	return &current, nil
}

func (s *SmartContract) GetSettlement(ctx contractapi.TransactionContextInterface, marketID string) (*Settlement, error) {
	marketID = strings.TrimSpace(marketID)
	if marketID == "" {
		return nil, fmt.Errorf("market_id is required")
	}

	allSettlements, err := s.getAllSettlements(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed reading settlements: %w", err)
	}

	settlement, exists := allSettlements[marketID]
	if !exists {
		empty := Settlement{Orders: map[string][]Order{}}
		return &empty, nil
	}

	return &settlement, nil
}

func (s *SmartContract) CanSendToAuction(ctx contractapi.TransactionContextInterface, auctionID string, period int) (bool, error) {
	auctionID = strings.TrimSpace(auctionID)
	if auctionID == "" {
		return false, fmt.Errorf("auction_id is required")
	}
	if period < 1 || period > 24 {
		return false, fmt.Errorf("period must be between 1 and 24")
	}

	auction, err := s.getAuction(ctx, auctionID)
	if err != nil {
		return false, err
	}
	if auction.Status != statusOpen {
		return false, fmt.Errorf("auction %s is not OPEN", auctionID)
	}

	market, err := s.getMarket(ctx, auction.IDMarket)
	if err != nil {
		return false, err
	}
	if market.Status != statusOpen {
		return false, fmt.Errorf("market %s is not OPEN", market.ID)
	}

	if period < auction.PeriodBegin || period > auction.PeriodEnd {
		return false, fmt.Errorf("period %d is outside auction %s range [%d, %d]",
			period, auctionID, auction.PeriodBegin, auction.PeriodEnd)
	}

	return true, nil
}

// =========================
// Private Data Transactions
// =========================

// AddOrder stores the order in the submitter org's private collection.
// blocksJSON must be a JSON array, e.g.:
// [{"amount":10,"price":15.5},{"amount":5,"price":16.0}]
//
// If blocksJSON is empty, the function also tries transient input under key "blocks".
func (s *SmartContract) AddOrder(ctx contractapi.TransactionContextInterface, idAuction string, period int, orderType string, blocksJSON string, providedHash string) (string, error) {
	canSend, err := s.CanSendToAuction(ctx, idAuction, period)
	if err != nil {
		return "", err
	}
	if !canSend {
		return "", fmt.Errorf("cannot send order to auction %s for period %d", idAuction, period)
	}

	orderType = strings.ToUpper(strings.TrimSpace(orderType))
	if orderType != orderTypeBuy && orderType != orderTypeSell {
		return "", fmt.Errorf("type must be BUY or SELL")
	}

	providedHash = strings.ToLower(strings.TrimSpace(providedHash))
	if providedHash == "" {
		return "", fmt.Errorf("hash is required")
	}
	if len(providedHash) != 64 {
		return "", fmt.Errorf("hash must be a sha256 hex string")
	}

	if strings.TrimSpace(blocksJSON) == "" {
		transientMap, err := ctx.GetStub().GetTransient()
		if err != nil {
			return "", fmt.Errorf("failed reading transient map: %w", err)
		}
		if v, ok := transientMap["blocks"]; ok {
			blocksJSON = string(v)
		}
	}

	if strings.TrimSpace(blocksJSON) == "" {
		return "", fmt.Errorf("blocks are required")
	}

	var blocks []Block
	if err := json.Unmarshal([]byte(blocksJSON), &blocks); err != nil {
		return "", fmt.Errorf("invalid blocks JSON: %w", err)
	}

	if err := validateBlocks(blocks); err != nil {
		return "", err
	}

	computedHash, err := computeOrderDigest(idAuction, period, orderType, blocks)
	if err != nil {
		return "", fmt.Errorf("failed computing order hash: %w", err)
	}
	if providedHash != computedHash {
		return "", fmt.Errorf("hash mismatch: provided=%s computed=%s", providedHash, computedHash)
	}

	clientID, err := ctx.GetClientIdentity().GetID()
	if err != nil {
		return "", fmt.Errorf("failed getting client ID: %w", err)
	}

	collection, err := s.getCollectionNameForClient(ctx)
	if err != nil {
		return "", err
	}

	orderID := ctx.GetStub().GetTxID()
	order := Order{
		IDOrder:   orderID,
		IDAuction: idAuction,
		Period:    period,
		Type:      orderType,
		Blocks:    blocks,
		ClientID:  clientID,
	}

	orderBytes, err := json.Marshal(order)
	if err != nil {
		return "", fmt.Errorf("failed marshalling order: %w", err)
	}

	privateKey := buildOrderPrivateKey(orderID)
	if err := ctx.GetStub().PutPrivateData(collection, privateKey, orderBytes); err != nil {
		return "", fmt.Errorf("failed storing order in collection %s: %w", collection, err)
	}

	if err := s.saveVerityHash(ctx, orderID, computedHash); err != nil {
		return "", fmt.Errorf("failed storing verity hash: %w", err)
	}

	eventPayload, err := json.Marshal(AcceptedOrderEvent{IDOrder: orderID})
	if err != nil {
		return "", fmt.Errorf("failed marshalling AcceptedOrder event: %w", err)
	}
	if err := ctx.GetStub().SetEvent("AcceptedOrder", eventPayload); err != nil {
		return "", fmt.Errorf("failed emitting AcceptedOrder event: %w", err)
	}

	return orderID, nil
}

func (s *SmartContract) GetOrdersByAuction(ctx contractapi.TransactionContextInterface, auctionID string) ([]Order, error) {
	if err := s.assertAuctioneer(ctx); err != nil {
		return nil, err
	}

	auctionID = strings.TrimSpace(auctionID)
	if auctionID == "" {
		return nil, fmt.Errorf("auction_id is required")
	}

	auction, err := s.getAuction(ctx, auctionID)
	if err != nil {
		return nil, err
	}
	if auction.Status != statusClosed {
		return nil, fmt.Errorf("auction %s is not CLOSED", auctionID)
	}

	collections := []string{
		collectionOrg1Auctioneer,
		collectionOrg2Auctioneer,
	}

	var orders []Order
	seen := make(map[string]struct{})

	for _, collection := range collections {
		iter, err := ctx.GetStub().GetPrivateDataByRange(
			collection,
			rangeStart(orderKeyPrefix),
			rangeEnd(orderKeyPrefix),
		)
		if err != nil {
			return nil, fmt.Errorf("failed querying private data in collection %s: %w", collection, err)
		}
		defer iter.Close()

		for iter.HasNext() {
			kv, err := iter.Next()
			if err != nil {
				return nil, fmt.Errorf("failed iterating orders in collection %s: %w", collection, err)
			}

			var order Order
			if err := json.Unmarshal(kv.Value, &order); err != nil {
				return nil, fmt.Errorf("failed unmarshalling order %s from collection %s: %w", kv.Key, collection, err)
			}

			if order.IDAuction != auctionID {
				continue
			}

			if _, exists := seen[order.IDOrder]; exists {
				continue
			}
			seen[order.IDOrder] = struct{}{}

			orders = append(orders, order)
		}
	}

	sort.Slice(orders, func(i, j int) bool {
		if orders[i].Period == orders[j].Period {
			return orders[i].IDOrder < orders[j].IDOrder
		}
		return orders[i].Period < orders[j].Period
	})

	return orders, nil
}

/* TODO: Add a function that only the auctionneer can read all the orders for a giving session*/

// =========================
// Helpers
// =========================

func (s *SmartContract) assertAuctioneer(ctx contractapi.TransactionContextInterface) error {
	mspID, err := ctx.GetClientIdentity().GetMSPID()
	if err != nil {
		return fmt.Errorf("failed getting client MSPID: %w", err)
	}
	if mspID != auctioneerMSP {
		return fmt.Errorf("access denied: function allowed only for MSP %s, caller MSP is %s", auctioneerMSP, mspID)
	}
	return nil
}

func (s *SmartContract) getCollectionNameForClient(ctx contractapi.TransactionContextInterface) (string, error) {
	mspID, err := ctx.GetClientIdentity().GetMSPID()
	if err != nil {
		return "", fmt.Errorf("failed getting client MSPID: %w", err)
	}

	switch mspID {
	case org1MSP:
		return collectionOrg1Auctioneer, nil
	case org2MSP:
		return collectionOrg2Auctioneer, nil
	default:
		return "", fmt.Errorf("no private data collection configured for caller MSP %s", mspID)
	}
}

func (s *SmartContract) stateExists(ctx contractapi.TransactionContextInterface, key string) (bool, error) {
	b, err := ctx.GetStub().GetState(key)
	if err != nil {
		return false, err
	}
	return b != nil, nil
}

func (s *SmartContract) getMarket(ctx contractapi.TransactionContextInterface, marketID string) (*Market, error) {
	marketID = strings.TrimSpace(marketID)
	if marketID == "" {
		return nil, fmt.Errorf("market_id is required")
	}

	key := buildMarketKey(marketID)
	b, err := ctx.GetStub().GetState(key)
	if err != nil {
		return nil, fmt.Errorf("failed reading market %s: %w", marketID, err)
	}
	if b == nil {
		return nil, fmt.Errorf("market %s does not exist", marketID)
	}

	var market Market
	if err := json.Unmarshal(b, &market); err != nil {
		return nil, fmt.Errorf("failed unmarshalling market %s: %w", marketID, err)
	}
	return &market, nil
}

func (s *SmartContract) putMarket(ctx contractapi.TransactionContextInterface, market *Market) error {
	if market == nil {
		return fmt.Errorf("market is nil")
	}
	if strings.TrimSpace(market.ID) == "" {
		return fmt.Errorf("market id is required")
	}
	if !isValidMarketStatus(market.Status) {
		return fmt.Errorf("invalid market status: %s", market.Status)
	}

	b, err := json.Marshal(market)
	if err != nil {
		return fmt.Errorf("failed marshalling market %s: %w", market.ID, err)
	}
	return ctx.GetStub().PutState(buildMarketKey(market.ID), b)
}

func (s *SmartContract) getAuction(ctx contractapi.TransactionContextInterface, auctionID string) (*Auction, error) {
	auctionID = strings.TrimSpace(auctionID)
	if auctionID == "" {
		return nil, fmt.Errorf("auction_id is required")
	}

	key := buildAuctionKey(auctionID)
	b, err := ctx.GetStub().GetState(key)
	if err != nil {
		return nil, fmt.Errorf("failed reading auction %s: %w", auctionID, err)
	}
	if b == nil {
		return nil, fmt.Errorf("auction %s does not exist", auctionID)
	}

	var auction Auction
	if err := json.Unmarshal(b, &auction); err != nil {
		return nil, fmt.Errorf("failed unmarshalling auction %s: %w", auctionID, err)
	}
	return &auction, nil
}

func (s *SmartContract) putAuction(ctx contractapi.TransactionContextInterface, auction *Auction) error {
	if auction == nil {
		return fmt.Errorf("auction is nil")
	}
	if strings.TrimSpace(auction.ID) == "" {
		return fmt.Errorf("auction id is required")
	}
	if strings.TrimSpace(auction.IDMarket) == "" {
		return fmt.Errorf("auction id_market is required")
	}
	if auction.PeriodBegin < 1 || auction.PeriodBegin > 24 {
		return fmt.Errorf("invalid period_begin: %d", auction.PeriodBegin)
	}
	if auction.PeriodEnd < 1 || auction.PeriodEnd > 24 {
		return fmt.Errorf("invalid period_end: %d", auction.PeriodEnd)
	}
	if auction.PeriodBegin > auction.PeriodEnd {
		return fmt.Errorf("period_begin cannot be greater than period_end")
	}
	if !isValidAuctionStatus(auction.Status) {
		return fmt.Errorf("invalid auction status: %s", auction.Status)
	}

	b, err := json.Marshal(auction)
	if err != nil {
		return fmt.Errorf("failed marshalling auction %s: %w", auction.ID, err)
	}
	return ctx.GetStub().PutState(buildAuctionKey(auction.ID), b)
}

func (s *SmartContract) getVerity(ctx contractapi.TransactionContextInterface) (Verity, error) {
	b, err := ctx.GetStub().GetState(verityStateKey)
	if err != nil {
		return Verity{}, err
	}
	if b == nil {
		return Verity{OrderHashes: map[string]string{}}, nil
	}

	var verity Verity
	if err := json.Unmarshal(b, &verity); err != nil {
		return Verity{}, err
	}
	if verity.OrderHashes == nil {
		verity.OrderHashes = map[string]string{}
	}

	return verity, nil
}

func (s *SmartContract) putVerity(ctx contractapi.TransactionContextInterface, verity *Verity) error {
	if verity == nil {
		return fmt.Errorf("verity is nil")
	}
	if verity.OrderHashes == nil {
		verity.OrderHashes = map[string]string{}
	}

	b, err := json.Marshal(verity)
	if err != nil {
		return err
	}
	return ctx.GetStub().PutState(verityStateKey, b)
}

func (s *SmartContract) getAllSettlements(ctx contractapi.TransactionContextInterface) (Settlements, error) {
	b, err := ctx.GetStub().GetState(settlementsStateKey)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return Settlements{}, nil
	}

	var settlements Settlements
	if err := json.Unmarshal(b, &settlements); err != nil {
		return nil, err
	}
	if settlements == nil {
		settlements = Settlements{}
	}
	return settlements, nil
}

func (s *SmartContract) putAllSettlements(ctx contractapi.TransactionContextInterface, settlements Settlements) error {
	if settlements == nil {
		settlements = Settlements{}
	}

	b, err := json.Marshal(settlements)
	if err != nil {
		return err
	}
	return ctx.GetStub().PutState(settlementsStateKey, b)
}

func validateBlocks(blocks []Block) error {
	if len(blocks) == 0 {
		return fmt.Errorf("blocks must not be empty")
	}

	// for i, b := range blocks {
	// if b.Amount <= 0 {
	// 	return fmt.Errorf("blocks[%d].amount must be > 0", i)
	// }
	// }

	return nil
}

func validateSettlement(s Settlement) error {
	if s.Orders == nil {
		return nil
	}

	for hour, orders := range s.Orders {
		h, err := strconv.Atoi(hour)
		if err != nil {
			return fmt.Errorf("invalid hour key %q: must be a number between 1 and 24", hour)
		}
		if h < 1 || h > 24 {
			return fmt.Errorf("invalid hour key %q: must be between 1 and 24", hour)
		}

		for i, o := range orders {
			if strings.TrimSpace(o.IDOrder) == "" {
				return fmt.Errorf("orders[%s][%d].id_order is required", hour, i)
			}
			if strings.TrimSpace(o.IDAuction) == "" {
				return fmt.Errorf("orders[%s][%d].id_auction is required", hour, i)
			}
			if o.Period < 1 || o.Period > 24 {
				return fmt.Errorf("orders[%s][%d].period must be between 1 and 24", hour, i)
			}
			oType := strings.ToUpper(strings.TrimSpace(o.Type))
			if oType != orderTypeBuy && oType != orderTypeSell {
				return fmt.Errorf("orders[%s][%d].type must be BUY or SELL", hour, i)
			}
		}
	}

	return nil
}

func isValidMarketStatus(status string) bool {
	switch status {
	case statusNewlyCreated, statusOpen, statusClosed:
		return true
	default:
		return false
	}
}

func isValidAuctionStatus(status string) bool {
	switch status {
	case statusNewlyCreated, statusOpen, statusClosed:
		return true
	default:
		return false
	}
}

func buildMarketKey(marketID string) string {
	return marketKeyPrefix + marketID
}

func buildAuctionKey(auctionID string) string {
	return auctionKeyPrefix + auctionID
}

func buildOrderPrivateKey(orderID string) string {
	return orderKeyPrefix + orderID
}

func buildVerityKey(orderID string) string {
	return verityKeyPrefix + orderID
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
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

func (s *SmartContract) saveVerityHash(ctx contractapi.TransactionContextInterface, orderID string, hash string) error {
	record := OrderHashRecord{
		OrderID:   orderID,
		Hash:      strings.ToLower(strings.TrimSpace(hash)),
		Algorithm: "SHA-256",
	}

	b, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed marshalling verity record: %w", err)
	}

	return ctx.GetStub().PutState(buildVerityKey(orderID), b)
}

func rangeStart(prefix string) string {
	return prefix
}

func rangeEnd(prefix string) string {
	return prefix + "\uffff"
}
