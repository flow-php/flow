{
  description = "PHP Development Environment";

  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    systems.url = "github:nix-systems/default";
    pkgs-by-name-for-flake-parts.url = "github:drupol/pkgs-by-name-for-flake-parts";
  };

  outputs =
    inputs@{ flake-parts, systems, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = import systems;

      imports = [
        inputs.pkgs-by-name-for-flake-parts.flakeModule
        ./.nix/pkgs.nix
      ];

      perSystem =
        { pkgs, config, ... }:
        let
          # Change the PHP version here
          php = config.packages.my-php.override { php = pkgs.php82; };
        in
        {
          devShells.default = pkgs.mkShell {
            shellHook = ''
              echo "$(php -v | head -n 1)"
              echo "Composer Version: $(composer --version)"
              echo "Checking compression extensions:"
              echo "  Snappy: $(php -m | grep -i snappy || echo "not loaded")"
              echo "  Brotli: $(php -m | grep -i brotli || echo "not loaded")"
              echo "  LZ4: $(php -m | grep -i lz4 || echo "not loaded")"
              echo "  Zstd: $(php -m | grep -i zstd || echo "not loaded")"
            '';

            packages = [
              php
              php.packages.composer
            ];
          };
        };
    };
}
