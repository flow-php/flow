{
    lib,
    stdenvNoCC,
    fetchurl,
}:

let
    version = "1.48.0";

    targets = {
        aarch64-darwin = { name = "aarch64-apple-darwin"; hash = "sha256-G4EVOAWkhQcuy+sUsrKqokqb/rRnACp9cly/wBy7GBE="; };
        x86_64-darwin = { name = "x86_64-apple-darwin"; hash = "sha256-nG8YzWQDbSnZYLB2qmNbY3JgxrSyGnvvkvuM4GxLw0w="; };
        aarch64-linux = { name = "aarch64-unknown-linux-musl"; hash = "sha256-ETi6ixtR2KX4Qsf2fs8fdY5Li+ZuKWJg356Z6bRmxl8="; };
        x86_64-linux = { name = "x86_64-unknown-linux-musl"; hash = "sha256-2dopRz6Uo7nztlEJZXxpgNaix8b6WeEHA9xbcTpdnOc="; };
    };

    target = targets.${stdenvNoCC.hostPlatform.system} or (throw "fnox: unsupported system ${stdenvNoCC.hostPlatform.system}");
in
stdenvNoCC.mkDerivation {
    pname = "fnox";
    inherit version;

    src = fetchurl {
        url = "https://github.com/jdx/fnox/releases/download/v${version}/fnox-${target.name}.tar.gz";
        inherit (target) hash;
    };

    sourceRoot = ".";

    installPhase = ''
        install -Dm755 fnox $out/bin/fnox
    '';

    meta = {
        description = "Run commands with secrets from any provider";
        homepage = "https://fnox.jdx.dev";
        license = lib.licenses.mit;
        mainProgram = "fnox";
    };
}
