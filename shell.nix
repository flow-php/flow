let
    nixpkgs = fetchTarball {
        url = "https://github.com/NixOS/nixpkgs/archive/88a5945d774ff733c2ecd65247aba714a1731fe7.tar.gz";

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

    php = pkgs.callPackage ./.nix/pkgs/flow-php/package.nix {
        php = base-php;
        inherit php-snappy php-lz4 php-brotli php-zstd with-pcov with-xdebug with-blackfire;
    };
in
pkgs.mkShell {
    buildInputs = [
        php
        php.packages.composer
        pkgs.starship
        pkgs.figlet
        pkgs.symfony-cli
    ]
        ++ pkgs.lib.optional with-blackfire pkgs.blackfire
    ;

    shellHook = ''
    if [ -f "$PWD/.nix/shell/starship.toml" ]; then
        export STARSHIP_CONFIG="$PWD/.nix/shell/starship.toml"
    else
        export STARSHIP_CONFIG="$PWD/.nix/shell/starship.toml.dist"
    fi

    eval "$(${pkgs.starship}/bin/starship init bash)"

    clear
    figlet "Flow PHP"
    '';
}