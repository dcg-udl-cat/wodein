{
  description = "Dev shell with Go, GCC, and jq";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    systems.url = "github:nix-systems/default";
    devshell.url = "github:numtide/devshell";
    devshell.inputs.nixpkgs.follows = "nixpkgs";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    treefmt-nix.inputs.nixpkgs.follows = "nixpkgs";
    mkdocs-flake.url = "github:applicative-systems/mkdocs-flake";
  };

  outputs = inputs @ {
    flake-parts,
    systems,
    ...
  }:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = import systems;

      imports = [
        inputs.devshell.flakeModule
        inputs.treefmt-nix.flakeModule
        inputs.mkdocs-flake.flakeModules.default
      ];

      perSystem = {
        config,
        self,
        pkgs,
        ...
      }: let
        auctioneer = pkgs.callPackage ./gateways/auctioneer/package.nix {};
        client = pkgs.callPackage ./gateways/client/package.nix {};
        oracle = pkgs.callPackage ./gateways/oracle/package.nix {};

        mkImage = {
          name,
          drv,
        }:
          pkgs.dockerTools.buildImage {
            inherit name;
            tag = "latest";

            copyToRoot = pkgs.buildEnv {
              name = "${name}-root";
              paths = [drv pkgs.cacert];
              pathsToLink = ["/bin" "/etc/ssl"];
            };

            config = {
              Entrypoint = ["/bin/${name}"];
              Env = ["SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"];
            };
          };
      in {
        devshells.default = {
          packages = with pkgs; [
            curl
            go
            gcc
            jq
            golangci-lint
            statix
          ];

          commands = [
            {
              name = "clean";
              help = "Remove Fabric install artifacts (install-fabric.sh, fabric-samples/, temp.yaml)";
              command = ''
                set -e
                ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
                cd "$ROOT"
                rm -f install-fabric.sh temp.yaml
                rm -rf fabric-samples
              '';
            }
            {
              name = "stop";
              help = "Bring down Fabric test network if fabric-samples/ is present";
              command = ''
                set -e
                ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
                cd "$ROOT"
                if [ -d fabric-samples ]; then
                  ./fabric-samples/test-network/network.sh down || true
                else
                  echo "fabric-samples not found; nothing to stop."
                fi
              '';
            }
          ];
        };

	documentation.mkdocs-root = ./docs;
        # formatter = config.treefmt.build;

        treefmt = {
          projectRootFile = "flake.nix";
          programs = {
            gofmt.enable = true;
            alejandra.enable = true;
          };
          settings.global.excludes = [
            "result"
            "./result"
            ".git"
          ];
        };

        packages = {
          inherit auctioneer client oracle;
          auctioneer-image = mkImage {
            name = "auctioneer";
            drv = auctioneer;
          };
          oracle-image = mkImage {
            name = "oracle";
            drv = oracle;
          };
        };
      };
    };
}
