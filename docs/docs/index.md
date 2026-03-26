<!-- <script src="https://asciinema.org/a/S3k6bw7777EUIV1hscmVSvV7d.js" id="asciicast-S3k6bw7777EUIV1hscmVSvV7d" async="true"></script> -->
<!---->
## Overview

This project implements an energy-market workflow on top of Hyperledger Fabric. It models a flexible electricity market where an auctioneer creates daily markets, participants submit buy and sell block orders, and an oracle clears each closed auction to publish settlements.

The blockchain architecture is split into two layers:

- `chaincode/`: the smart contract that defines the market state machine and validation rules.
- `gateways/`: Go applications that act on behalf of the different actors in the system.

Each market is identified by a date in `dd-mm-yyyy` format and contains three auction sessions:

- `A1`: periods `1..24`
- `A2`: periods `1..24`
- `A3`: periods `13..24`

The lifecycle is the same across the whole project:

1. The auctioneer creates the market for a given day.
2. The auctioneer opens and closes the three sessions according to the configured schedule.
3. Clients submit `BUY` and `SELL` orders while an auction is open.
4. When an auction closes, the chaincode emits a `CloseAuction` event.
5. The oracle consumes that event, clears the order book, and updates the market settlement.
6. The verifier can later recompute order hashes and check that the stored settlement matches the original submitted data.

Fabric is used here to provide an auditable and shared state for all participants. The chaincode enforces the allowed transitions for markets and auctions, validates orders, emits close-auction events, and stores settlement and verity data on-chain.

## What does wodein offer?

`wodein` is exposed through several Go programs under `gateways/`, each one representing a different role in the market.

### Auctioneer daemon

`gateways/auctioneer` is the operational daemon for the market operator. It connects to Fabric and continuously reconciles the expected auction schedule with the ledger state.

Its responsibilities are:

- create the daily market if it does not exist yet
- open and close the three sessions automatically
- keep the market lifecycle aligned with the configured timezone

The implemented default schedule is:

- `A1`: opens at `14:00` and closes at `15:00` on the previous day
- `A2`: opens at `21:00` and closes at `22:00` on the previous day
- `A3`: opens at `09:00` and closes at `10:00` on the market day

### Client CLI

`gateways/client` is the participant-facing CLI. It is used by buyers and sellers to inspect the current market and submit orders.

Available commands:

- `list`: shows the currently open market and auction
- `create-order`: submits a `BUY` or `SELL` order for a given auction and period
- `settlement`: queries the settlement for a given auction

When creating an order, the client computes a SHA-256 digest of the submitted payload and sends that hash together with the order. This makes later verification possible.

### Oracle daemon

`gateways/oracle` is the settlement processor. It listens for `CloseAuction` events emitted by the chaincode, loads the orders of the closed auction, runs the clearing algorithm, and writes the resulting settlement back to the ledger.

Its current implementation:

- uses a checkpoint file so processing can resume after restarts
- applies a greedy order-book clearing algorithm
- can process several periods in parallel

### Verifier CLI

`gateways/verifier` is an integrity-check tool. It reads the settlement of a market, fetches the original orders, recomputes their hashes, and compares them with the on-chain verity records.

Available command:

- `verify`: verifies all orders in a market settlement, or only the orders from a specific auction

### Chaincode

`chaincode/smartcontract.go` is the canonical business logic of the platform. It defines the ledger data model and exposes the transactions used by the gateways, including market creation, auction opening and closing, order submission, settlement updates, and verity registration.
