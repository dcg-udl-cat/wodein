# WODEIN

## Requirements

You need the following tools installed:

- [Nix](https://nixos.org/download/)
- [Docker](https://docs.docker.com/get-docker/)

Docker must be running before starting the blockchain containers.

## Development Shell

Enter the Nix development shell from the repository root:

```sh
nix develop
```

This provides Go and the other packages and scripts used by the project.

## Start the Blockchain

After entering the development shell, start the Hyperledger Fabric containers with:

```sh
start-blockchain
```

## Build and Run the Oracle

Build the oracle package:

```sh
nix build .#oracle --out-link oracle
```

Execute it after the blockchain has been started:

```sh
./oracle/bin/oracle
```

## Build and Run the Benchmark

Build the benchmark package:

```sh
nix build .#blockchain-benchmark --out-link benchmark
```

Execute it after the blockchain and oracle has been started:

```sh
./benchmark/bin/blockchain-benchmark --dataset {dataset_path}
```

## Other Packages

You can also build and run the other packages normally. List the available
flake outputs with:

```sh
nix flake show
```

Then read the `README.md` for each package. Those package-specific READMEs
document the required environment variables and runtime details.
