{
  description = "PHP Development Environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        snappy = pkgs.callPackage ./.nix/snappy.nix {
          buildPecl = pkgs.php82.buildPecl;
          fetchgit = pkgs.fetchgit;
        };

        lz4 = pkgs.callPackage ./.nix/lz4.nix {
          buildPecl = pkgs.php82.buildPecl;
          fetchgit = pkgs.fetchgit;
        };

       brotli = pkgs.callPackage ./.nix/brotli.nix {
          buildPecl = pkgs.php82.buildPecl;
          fetchgit = pkgs.fetchgit;
        };

       zstd = pkgs.callPackage ./.nix/zstd.nix {
          buildPecl = pkgs.php82.buildPecl;
          fetchgit = pkgs.fetchgit;
        };

        # Create a custom PHP package with required extensions
        php = pkgs.php82.buildEnv {
          extensions = { all, enabled }: with all; enabled ++ [
            bcmath
            dom
            mbstring
            xmlreader
            xmlwriter
            zlib
            pcov
            snappy
            lz4
            brotli
            zstd
            pcov
          ];

          extraConfig = ''
                date.timezone = UTC
                max_execution_time = 1800
                max_input_time = 3600
                max_input_nesting_level = 64
                memory_limit = -1
                post_max_size = 200M
                upload_max_filesize = 150M
                file_uploads = On
                max_file_uploads = 20
                short_open_tag = off
          '';
        };

      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            php
            php.packages.composer
            pkgs.figlet
            pkgs.btop
            pkgs.git
          ];

          shellHook = ''
            figlet "Flow PHP"
            echo "$(php -v | head -n 1)"
            echo "Composer Version: $(composer --version)"
            echo "Checking compression extensions:"
            echo "  Snappy: $(php -m | grep -i snappy || echo "not loaded")"
            echo "  Brotli: $(php -m | grep -i brotli || echo "not loaded")"
            echo "  LZ4: $(php -m | grep -i lz4 || echo "not loaded")"
            echo "  Zstd: $(php -m | grep -i zstd || echo "not loaded")"
          '';
        };
      });
}