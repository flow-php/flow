let
    nixpkgs = fetchTarball {
        # Oct 31, 2025
        url = "https://github.com/NixOS/nixpkgs/archive/66a437ebcf6160152336e801a7ec289ba2aba3c5.tar.gz";
    };

    lockedPkgs = import nixpkgs {
        config = {
            allowUnfree = true;
        };
    };
in
{
    pkgs ? lockedPkgs,
    php-version ? 8.2,
    with-blackfire ? false,
    with-xdebug ? false,
    with-pcov ? !with-blackfire,
    with-pg-query-ext ? false,
    with-terraform ? false,
    with-wasm ? false,
}:

let
    base-php = if php-version == 8.2 then
        pkgs.php82
    else if php-version == 8.3 then
        pkgs.php83
    else if php-version == 8.4 then
        pkgs.php84
    else
        throw "Unknown php version ${php-version}";

    php-brotli = pkgs.callPackage ./.nix/pkgs/php-brotli/package.nix { php = base-php; };
    php-snappy = pkgs.callPackage ./.nix/pkgs/php-snappy/package.nix { php = base-php; };
    php-lz4 = pkgs.callPackage ./.nix/pkgs/php-lz4/package.nix { php = base-php; };
    php-zstd = pkgs.callPackage ./.nix/pkgs/php-zstd/package.nix { php = base-php; };
    php-pg-query-ext = pkgs.callPackage ./.nix/pkgs/php-pg-query-ext/package.nix { php = base-php; };

    php = pkgs.callPackage ./.nix/pkgs/flow-php/package.nix {
        php = base-php;
        inherit php-snappy php-lz4 php-brotli php-zstd php-pg-query-ext with-pcov with-xdebug with-blackfire with-pg-query-ext;
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
    ]
        ++ pkgs.lib.optional with-blackfire pkgs.blackfire
        ++ pkgs.lib.optionals with-wasm [
            # WASM build tools
            pkgs.emscripten
            pkgs.autoconf
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
        ++ pkgs.lib.optionals with-pg-query-ext [
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
    ;

    shellHook = ''
    if [ -f "$PWD/.nix/shell/starship.toml" ]; then
        export STARSHIP_CONFIG="$PWD/.nix/shell/starship.toml"
    else
        export STARSHIP_CONFIG="$PWD/.nix/shell/starship.toml.dist"
    fi

    ${pkgs.lib.optionalString with-pg-query-ext ''
    # Setup for pg-query-ext extension development
    export PHP_CONFIG="${php}/bin/php-config"
    export PHPIZE="${php.unwrapped.dev}/bin/phpize"
    ''}

    eval "$(${pkgs.starship}/bin/starship init bash)"

    clear
    figlet "Flow PHP"
    '';
}