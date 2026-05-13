{pkgs ? import <nixpkgs> {}}:
pkgs.buildGoModule {
  pname = "blockchain-benchmark";
  version = "0.1.0";

  src = ./.;

  vendorHash = "sha256-hpoCUG1ewFAhId7tG9S3aNgqM95pUL+iGYqhGIFirbg=";

  ldflags = [
    "-s"
    "-w"
  ];

  postInstall = ''
    mv "$out/bin/benchmark.go" "$out/bin/blockchain-benchmark"
  '';

  meta.mainProgram = "blockchain-benchmark";
}
