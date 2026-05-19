{
    php-version ? 8.3,
    with-blackfire ? false,
    with-xdebug ? false,
    with-pcov ? !with-blackfire,
    with-pg-query-ext ? !with-c,
    with-arrow-ext ? !with-rust,
    with-c ? false,
    with-rust ? false,
    with-terraform ? false,
    with-wasm ? false,
    with-grpc ? true,
    with-protobuf ? true,
    with-protoc ? false
}:

assert (!(with-rust && with-arrow-ext)) || builtins.throw "Cannot use --arg with-rust true and --arg with-arrow-ext true together. Use: --arg with-arrow-ext false --arg with-rust true";
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
            # Blackfire upstream republishes the same versioned tarball with different
            # bytes when they rebuild, invalidating the sha256 pinned in nixpkgs. Override
            # just the CLI agent's src on macOS arm64 with the current upstream hash.
            # Linux and other platforms keep nixpkgs' original src untouched. The PHP
            # extension (php83.extensions.blackfire) uses a separate upstream URL and is
            # unaffected on every platform.
            (final: prev:
                if prev.stdenv.hostPlatform.system == "aarch64-darwin" then {
                    blackfire = prev.blackfire.overrideAttrs (old: {
                        src = prev.fetchurl {
                            url = "https://packages.blackfire.io/blackfire/2.29.7/blackfire-darwin_arm64.pkg.tar.gz";
                            sha256 = "sha256-e0oTxGFxgURMyUoTNh+NFGVoO9qGKrHNKud3IFD0fec=";
                        };
                    });
                } else {}
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

    php = pkgs.callPackage ./.nix/pkgs/flow-php/package.nix {
        php = base-php;
        inherit php-snappy php-lz4 php-brotli php-zstd php-pg-query-ext php-arrow-ext
                with-pcov with-xdebug with-blackfire with-pg-query-ext with-arrow-ext with-grpc with-protobuf;
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
        pkgs.just
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