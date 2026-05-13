# Auctioneer Gateway

## Purpose

Runs a daemon that keeps market auctions aligned with the configured schedule: it creates tomorrow's market after 11:00, opens auction sessions when their time window starts, and closes them when their time window ends.

## How to Execute

From this directory:

```sh
go run .
```

With Nix:

```sh
nix-build package.nix
./result/bin/auctioneer
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
| `APP_TIMEZONE` | `Europe/Madrid` |
| `APP_LOG_LEVEL` | `INFO` |
| `APP_POLL_INTERVAL` | `30s` |
| `FABRIC_EVALUATE_TIMEOUT` | `5s` |
| `FABRIC_ENDORSE_TIMEOUT` | `15s` |
| `FABRIC_SUBMIT_TIMEOUT` | `5s` |
| `FABRIC_COMMIT_STATUS_TIMEOUT` | `1m` |
