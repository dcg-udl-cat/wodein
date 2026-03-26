{pkgs ? import <nixpkgs> {}}:
pkgs.buildGoModule {
  pname = "client";
  version = "0.1.0";

  src = ./.;

  vendorHash = "sha256-hpoCUG1ewFAhId7tG9S3aNgqM95pUL+iGYqhGIFirbg=";

  ldflags = [
    "-s"
    "-w"
  ];

  postInstall = ''
    mv "$out/bin/main.go" "$out/bin/client"
  '';

  meta.mainProgram = "client";
}
