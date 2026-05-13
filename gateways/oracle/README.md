# Oracle Gateway

## Purpose

Runs a settlement listener for `CloseAuction` chaincode events. When an auction closes, the active oracle reads its orders, clears matched buy and sell blocks with the greedy order-book algorithm, and writes the settlement back to the ledger.

## How to Execute

From this directory:

```sh
go run .
```

With Nix:

```sh
nix-build package.nix
./result/bin/oracle
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
| `APP_CHECKPOINT_FILE` | `./close-auction.checkpoint` |
| `APP_LOG_LEVEL` | `INFO` |
| `APP_RECONNECT_DELAY` | `3s` |
| `APP_MAX_PARALLEL_PERIODS` | `4` |
| `APP_ORACLE_CLUSTER_CONFIG` | unset |
| `FABRIC_EVALUATE_TIMEOUT` | `5s` |
| `FABRIC_ENDORSE_TIMEOUT` | `15s` |
| `FABRIC_SUBMIT_TIMEOUT` | `5s` |
| `FABRIC_COMMIT_STATUS_TIMEOUT` | `1m` |

If `APP_ORACLE_CLUSTER_CONFIG` is set, it must point to a JSON file with `node_id`, `listen_address`, `peers`, `heartbeat_interval`, and `peer_timeout`; see `oracle-cluster.example.json`.
