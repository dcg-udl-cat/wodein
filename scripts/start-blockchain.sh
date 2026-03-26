#!/bin/sh

set -e

# Ensure we run from repo root regardless of caller location
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Install fabric binaries and images
curl -sSLO https://raw.githubusercontent.com/hyperledger/fabric/main/scripts/install-fabric.sh && chmod +x install-fabric.sh
./install-fabric.sh

# Create users for testing

change_user() {
  awk -v new_count="$2" '
  /Users:/ { print; found=1; next }
  found && /Count:/ { sub(/[0-9]+/, new_count); found=0 }
  { print }
  ' "$1" > temp.yaml && mv temp.yaml "$1"
}

echo "Adding users to be created"
YAML_ORG_2="./fabric-samples/test-network/organizations/cryptogen/crypto-config-org2.yaml"
YAML_ORG_3="./fabric-samples/test-network/addOrg3/org3-crypto.yaml"
N_USERS=2000

change_user "$YAML_ORG_2" "$N_USERS"
change_user "$YAML_ORG_3" "$N_USERS"

# Start channel and network
./fabric-samples/test-network/network.sh up createChannel

# Add Organization 3 to the network
cd ./fabric-samples/test-network/addOrg3
./addOrg3.sh up
cd "$ROOT_DIR"

# Deploy the chaincode.
CHAINCODE_DIR="${ROOT_DIR}/chaincode"
./fabric-samples/test-network/network.sh deployCC -ccn market -ccp "${CHAINCODE_DIR}" -ccl go -cccg "${CHAINCODE_DIR}/collections_config.json"

# The auctioneer creates a market and starts the first auction on it
# ./fabric-samples/test-network/network.sh cc invoke -c mychannel -ccn market -ccic '{"Args":["GetMarkets"]}'
# sleep 2
# ./fabric-samples/test-network/network.sh cc invoke -c mychannel -ccn market -ccic '{"Args":["CreateMarket","01-01-2002"]}'
# sleep 2
# ./fabric-samples/test-network/network.sh cc invoke -c mychannel -ccn market -ccic '{"Args":["OpenAuction","01-01-2001-A1"]}'
# sleep 2
# # An organization executes a buy on it.
# OVERRIDE_ORG=2 ./fabric-samples/test-network/network.sh cc invoke -ccn market -c mychannel -ccic '{"Args":["AddOrder","01-01-2001-A1","3","BUY","[{\"price\":10,\"amount\":5}]"]}'
# sleep 2
# OVERRIDE_ORG=1 ./fabric-samples/test-network/network.sh cc invoke -ccn market -c mychannel -ccic '{"Args":["CloseAuction","01-01-2001-A1"]}'
# OVERRIDE_ORG=1 ./fabric-samples/test-network/network.sh cc invoke -ccn market -c mychannel -ccic '{"Args":["CloseAuction","01-01-2001-A1"]}'
