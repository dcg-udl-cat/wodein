# Client Gateway

## Purpose

Provides a CLI for market participants to inspect the currently open market and auction, submit buy or sell orders with a SHA-256 order hash, and view settlement entries for an auction.

## How to Execute

From this directory:

```sh
go run . list
go run . create-order --auction <auction-id> --period <1-24> --type BUY|SELL --blocks '[{"amount":10,"price":15.5}]'
go run . create-order --auction <auction-id> --period <1-24> --type BUY|SELL --blocks-file blocks.json
go run . settlement --auction <auction-id>
```

With Nix:

```sh
nix-build package.nix
./result/bin/client list
```

Environment variables:

| Variable | Default |
| --- | --- |
| `FABRIC_CHANNEL_NAME` | `mychannel` |
| `FABRIC_CHAINCODE_NAME` | `market` |
| `FABRIC_MSP_ID` | `Org2MSP` |
| `FABRIC_PEER_ENDPOINT` | `dns:///localhost:9051` |
| `FABRIC_PEER_TLS_HOST_OVERRIDE` | `peer0.org2.example.com` |
| `FABRIC_CLIENT_CERT_PATH` | `../../fabric-samples/test-network/organizations/peerOrganizations/org2.example.com/users/User1@org2.example.com/msp/signcerts` |
| `FABRIC_CLIENT_KEY_PATH` | `../../fabric-samples/test-network/organizations/peerOrganizations/org2.example.com/users/User1@org2.example.com/msp/keystore` |
| `FABRIC_PEER_TLS_ROOTCERT_PATH` | `../../fabric-samples/test-network/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt` |
| `FABRIC_EVALUATE_TIMEOUT` | `10s` |
| `FABRIC_ENDORSE_TIMEOUT` | `15s` |
| `FABRIC_SUBMIT_TIMEOUT` | `15s` |
| `FABRIC_COMMIT_STATUS_TIMEOUT` | `1m` |
