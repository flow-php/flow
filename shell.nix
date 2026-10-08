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
        url = "https://github.com/NixOS/nixpkgs/archive/00455b0a3690d3f5dc61e9aef4277dc86235b73f.tar.gz";
        sha256 = "0vz9jxm81zl356vvqaswlf8fqgsz6p5yjy0iy5qq7kr81kpfvbch";
    };

    pkgs = import nixpkgs {
        config = {
            allowUnfree = true;
        };
        overlays = [
            (final: prev:
                if prev.stdenv.hostPlatform.system == "aarch64-darwin" then {
                    blackfire = prev.blackfire.overrideAttrs (old: {
                        version = "2026.9.1";
                        src = prev.fetchurl {
                            url = "https://packages.blackfire.io/blackfire/2026.9.1/blackfire-darwin_arm64.pkg.tar.gz";
                            sha256 = "sha256-xNn78U4jdABzWrSKMSSZXE5tuf/SRK8OwdhldKBBKk0=";
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
    extension-version = let
        lib = pkgs.lib;
        resolve = base: path: if lib.hasPrefix "/" path then /. + path else base + "/${path}";
        firstLine = file: builtins.head (lib.splitString "\n" (builtins.readFile file));

        dotGit = ./. + "/.git";
        gitDir =
            if !(builtins.pathExists dotGit) then
                throw "cannot determine the extension version: ${toString ./.} is not a git checkout"
            else if lib.pathIsDirectory dotGit then
                dotGit
            else
                resolve ./. (lib.removePrefix "gitdir: " (firstLine dotGit));
        commonDir =
            if builtins.pathExists (gitDir + "/commondir") then resolve gitDir (firstLine (gitDir + "/commondir")) else gitDir;

        looseTags =
            lib.optionals (lib.pathIsDirectory (commonDir + "/refs/tags"))
                (builtins.attrNames (builtins.readDir (commonDir + "/refs/tags")));
        packedTags =
            lib.optionals (builtins.pathExists (commonDir + "/packed-refs"))
                (map builtins.head (lib.filter (match: match != null)
                    (map (builtins.match "[0-9a-f]+ refs/tags/(.+)") (lib.splitString "\n" (builtins.readFile (commonDir + "/packed-refs"))))));
        tags = lib.filter (tag: builtins.match "[0-9]+\\.[0-9]+\\.[0-9]+" tag != null) (looseTags ++ packedTags);
        latest = lib.splitString "." (lib.foldl' (a: b: if builtins.compareVersions a b >= 0 then a else b) (builtins.head tags) tags);
    in
        if builtins.pathExists (commonDir + "/reftable") then
            throw "cannot determine the extension version: reftable ref storage is not supported, run `git refs migrate --ref-format=files`"
        else if tags == [ ] then
            throw "cannot determine the extension version: no X.Y.Z tags in ${toString commonDir}, run `git fetch --tags`"
        else
            "${builtins.elemAt latest 0}.${toString (lib.toInt (builtins.elemAt latest 1) + 1)}.0-dev";

    php-arrow-ext = pkgs.callPackage ./.nix/pkgs/php-arrow-ext/package.nix { php = base-php; arrow-ext-version = extension-version; };
    php-flow-php-ext = pkgs.callPackage ./.nix/pkgs/php-flow-php-ext/package.nix { php = base-php; flow-php-ext-version = extension-version; };

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

    if [ -f "$PWD/.nix/blackfire/blackfire.env" ]; then
        set -a; . "$PWD/.nix/blackfire/blackfire.env"; set +a
    fi

    eval "$(${pkgs.starship}/bin/starship init bash)"

    clear
    figlet "Flow PHP"
    '';
}