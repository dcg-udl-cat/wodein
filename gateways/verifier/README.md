# Verifier Gateway

## Purpose

Provides a CLI that verifies settled orders by recomputing each order's SHA-256 hash from the original ledger order payload and comparing it with the on-chain hash record.

## How to Execute

From this directory:

```sh
go run . verify --market <market-id>
go run . verify --market <market-id> --auction <auction-id>
go run . verify --auction <auction-id>
```

Or build and run the binary:

```sh
go build -o verifier .
./verifier verify --market <market-id>
```

Environment variables:

| Variable | Default |
| --- | --- |
| `FABRIC_CHANNEL_NAME` | `mychannel` |
| `FABRIC_CHAINCODE_NAME` | `market` |
| `FABRIC_MSP_ID` | `Org1MSP` |
| `FABRIC_PEER_ENDPOINT` | `dns:///localhost:7051` |
| `FABRIC_PEER_TLS_HOST_OVERRIDE` | `peer0.org1.example.com` |
| `FABRIC_CLIENT_CERT_PATH` | `../../fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/signcerts` |
| `FABRIC_CLIENT_KEY_PATH` | `../../fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/keystore` |
| `FABRIC_PEER_TLS_ROOTCERT_PATH` | `../../fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt` |
| `FABRIC_EVALUATE_TIMEOUT` | `10s` |
| `FABRIC_ENDORSE_TIMEOUT` | `15s` |
| `FABRIC_SUBMIT_TIMEOUT` | `15s` |
| `FABRIC_COMMIT_STATUS_TIMEOUT` | `1m` |
