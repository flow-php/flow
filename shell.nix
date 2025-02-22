let
    nixpkgs = fetchTarball {
        url = "https://github.com/NixOS/nixpkgs/archive/d2e52032da935f4972579f132250f3c3f47136d1.tar.gz";
    };

    lockedPkgs = import nixpkgs {
        config = {
            allowUnfree = true;
        };
    };
in
{
    pkgs ? lockedPkgs,
    with-pcov ? true,
    with-xdebug ? false,
    with-blackfire ? false
}:

let
    basePHP = pkgs.php82;

    php-brotli = pkgs.callPackage ./.nix/pkgs/php-brotli/package.nix { php = basePHP; };
    php-snappy = pkgs.callPackage ./.nix/pkgs/php-snappy/package.nix { php = basePHP; };
    php-lz4 = pkgs.callPackage ./.nix/pkgs/php-lz4/package.nix { php = basePHP; };
    php-zstd = pkgs.callPackage ./.nix/pkgs/php-zstd/package.nix { php = basePHP; };

    php = pkgs.callPackage ./.nix/pkgs/flow-php/package.nix {
        php = basePHP;
        inherit php-snappy php-lz4 php-brotli php-zstd with-pcov with-xdebug with-blackfire;
    };
in
pkgs.mkShell {
    buildInputs = [
        php
        php.packages.composer
        pkgs.starship
        pkgs.figlet
    ];

    shellHook = ''
    if [ -f "./.nix/shell/starship.toml" ]; then
        export STARSHIP_CONFIG="./.nix/shell/starship.toml"
    else
        export STARSHIP_CONFIG="./.nix/shell/starship.toml.dist"
    fi

    eval "$(${pkgs.starship}/bin/starship init bash)"
    clear
    figlet "Flow PHP"
    '';
}