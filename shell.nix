{
    php-version ? 8.3,
    with-blackfire ? false,
    with-xdebug ? false,
    with-pcov ? !with-blackfire,
    with-pg-query-ext ? !with-c,
    with-arrow-ext ? !with-rust,
    with-flow-php-ext ? !with-rust,
    with-c ? false,
    with-rust ? false,
    with-terraform ? false,
    with-wasm ? false,
    with-grpc ? true,
    with-protobuf ? true,
    with-protoc ? false
}:

assert (!(with-rust && with-arrow-ext)) || builtins.throw "Cannot use --arg with-rust true and --arg with-arrow-ext true together. Use: --arg with-arrow-ext false --arg with-rust true";
assert (!(with-rust && with-flow-php-ext)) || builtins.throw "Cannot use --arg with-rust true and --arg with-flow-php-ext true together. Use: --arg with-flow-php-ext false --arg with-rust true";
assert (!(with-c && with-pg-query-ext)) || builtins.throw "Cannot use --arg with-c true and --arg with-pg-query-ext true together. Use: --arg with-pg-query-ext false --arg with-c true";

let
    nixpkgs = fetchTarball {
        url = "https://github.com/NixOS/nixpkgs/archive/0c4e77908e1204498184d81cda8716e1ba4c47af.tar.gz";
        sha256 = "0mbr776gj2qk9klvara4zlww3g0da4nfbnrmi114nnmmayx3pyj4";
    };

    pkgs = import nixpkgs {
        config = {
            allowUnfree = true;
        };
        overlays = [
            (final: prev:
                if prev.stdenv.hostPlatform.system == "aarch64-darwin" then
                    let
                        blackfire-probe-version = "2026.9.2";
                        blackfire-probe-hashes = {
                            "83" = "sha256-3oJtMuVKGUgpduMp7snSdGE/BH764EA7yz+N70+qFNg=";
                            "84" = "sha256-0HOCBB9dgU9Vq5/F0iKCzumjwT81qxHElPsLGKgVhr0=";
                            "85" = "sha256-Hi9bC/CigkA3VWFTqfE7JzBcGGJAhUwnmMndHgbFIW4=";
                        };
                        with-blackfire-probe = php-version: php: php.override {
                            packageOverrides = php-final: php-prev: {
                                extensions = php-prev.extensions // {
                                    blackfire = php-prev.extensions.blackfire.overrideAttrs (old: {
                                        version = blackfire-probe-version;
                                        src = prev.fetchurl {
                                            url = "https://packages.blackfire.io/binaries/blackfire-php/${blackfire-probe-version}/blackfire-php-darwin_arm64-php-${php-version}.so";
                                            hash = blackfire-probe-hashes.${php-version};
                                        };
                                    });
                                };
                            };
                        };
                    in {
                        blackfire = prev.blackfire.overrideAttrs (old: {
                            version = "2026.9.1";
                            src = prev.fetchurl {
                                url = "https://packages.blackfire.io/blackfire/2026.9.1/blackfire-darwin_arm64.pkg.tar.gz";
                                sha256 = "sha256-xNn78U4jdABzWrSKMSSZXE5tuf/SRK8OwdhldKBBKk0=";
                            };
                        });
                        php83 = with-blackfire-probe "83" prev.php83;
                        php84 = with-blackfire-probe "84" prev.php84;
                        php85 = with-blackfire-probe "85" prev.php85;
                    }
                else {}
            )
        ];
    };

    base-php = if php-version == 8.3 then
        pkgs.php83
    else if php-version == 8.4 then
        pkgs.php84
    else if php-version == 8.5 then
        pkgs.php85
    else
        throw "Unknown php version ${php-version}";

    php-brotli = pkgs.callPackage ./.nix/pkgs/php-brotli/package.nix { php = base-php; };
    php-snappy = pkgs.callPackage ./.nix/pkgs/php-snappy/package.nix { php = base-php; };
    php-lz4 = pkgs.callPackage ./.nix/pkgs/php-lz4/package.nix { php = base-php; };
    php-zstd = pkgs.callPackage ./.nix/pkgs/php-zstd/package.nix { php = base-php; };
    php-pg-query-ext = pkgs.callPackage ./.nix/pkgs/php-pg-query-ext/package.nix { php = base-php; };
    php-arrow-ext = pkgs.callPackage ./.nix/pkgs/php-arrow-ext/package.nix { php = base-php; };
    php-flow-php-ext = pkgs.callPackage ./.nix/pkgs/php-flow-php-ext/package.nix { php = base-php; };

    php = pkgs.callPackage ./.nix/pkgs/flow-php/package.nix {
        php = base-php;
        inherit php-snappy php-lz4 php-brotli php-zstd php-pg-query-ext php-arrow-ext php-flow-php-ext
                with-pcov with-xdebug with-blackfire with-pg-query-ext with-arrow-ext with-flow-php-ext with-grpc with-protobuf;
    };
in
pkgs.mkShell {
    buildInputs = [
        php
        php.packages.composer
        pkgs.starship
        pkgs.figlet
        pkgs.symfony-cli
        pkgs.act
        pkgs.hyperfine
        pkgs.actionlint
        pkgs.zizmor
        pkgs.just
        pkgs.oxipng
        pkgs.jpegoptim
    ]
        ++ pkgs.lib.optional with-blackfire pkgs.blackfire
        ++ pkgs.lib.optionals with-wasm [
            # WASM build tools
            pkgs.emscripten
            pkgs.autoconf
            pkgs.cmake
            pkgs.wget
            pkgs.gnutar
            pkgs.xz
            pkgs.libxml2
            pkgs.pkg-config
        ]
        ++ pkgs.lib.optionals with-terraform [
            # Terraform
            pkgs.terraform
            pkgs.nodejs_24
        ]
        ++ pkgs.lib.optionals with-c [
            # C development tools for pg-query-ext extension development
            pkgs.gcc
            pkgs.gnumake
            pkgs.autoconf
            pkgs.automake
            pkgs.libtool
            pkgs.protobuf
            pkgs.protobufc
            pkgs.git
            php.unwrapped.dev
        ]
        ++ pkgs.lib.optionals with-rust [
            # Rust development tools for arrow-ext extension development
            pkgs.rustc
            pkgs.cargo
            pkgs.rustfmt
            pkgs.clippy
            pkgs.clang
            pkgs.llvmPackages.libclang
            pkgs.pkg-config
            php.unwrapped.dev
        ]
        ++ pkgs.lib.optionals with-protoc [
            # protoc + grpc_php_plugin for regenerating protobuf/gRPC PHP classes
            pkgs.protobuf
            pkgs.grpc
            pkgs.git
        ]
    ;

    shellHook = ''
    if [ -f "$PWD/.nix/shell/starship.toml" ]; then
        export STARSHIP_CONFIG="$PWD/.nix/shell/starship.toml"
    else
        export STARSHIP_CONFIG="$PWD/.nix/shell/starship.toml.dist"
    fi

    ${pkgs.lib.optionalString with-c ''
    # Setup for pg-query-ext C extension development
    export PHP_CONFIG="${php.unwrapped.dev}/bin/php-config"
    export PHPIZE="${php.unwrapped.dev}/bin/phpize"
    ''}

    ${pkgs.lib.optionalString with-rust ''
    # Setup for arrow-ext Rust extension development
    export LIBCLANG_PATH="${pkgs.llvmPackages.libclang.lib}/lib"
    export PHP_CONFIG="${php.unwrapped.dev}/bin/php-config"
    export PHPIZE="${php.unwrapped.dev}/bin/phpize"
    ''}

    eval "$(${pkgs.starship}/bin/starship init bash)"

    clear
    figlet "Flow PHP"
    '';
}