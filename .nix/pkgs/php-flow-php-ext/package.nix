{
  php,
  lib,
  stdenv,
  rustPlatform,
  clang,
  llvmPackages,
  flow-php-ext-version ? "dev",
}:

let
  extSrc = builtins.path {
    path = ../../../src/extension/flow-php-ext;
    name = "flow-php-ext-src";
    filter = path: type:
      let baseName = baseNameOf path;
      in !(
        baseName == "target" ||
        baseName == "vendor" ||
        baseName == "ext" ||
        baseName == ".gitignore" ||
        baseName == ".gitattributes" ||
        baseName == "composer.json" ||
        baseName == "composer.lock"
      );
  };
  pkg = rustPlatform.buildRustPackage {
    pname = "php-flow-php-ext";
    version = flow-php-ext-version;

    src = extSrc;

    cargoLock.lockFile = ../../../src/extension/flow-php-ext/Cargo.lock;

    nativeBuildInputs = [
      clang
      llvmPackages.libclang
      php.unwrapped
      php.unwrapped.dev
    ];

    env = {
      LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
      PHP_CONFIG = "${php.unwrapped.dev}/bin/php-config";
      PHP = "${php.unwrapped}/bin/php";
      FLOW_PHP_EXT_VERSION = flow-php-ext-version;
    };

    installPhase = let
      targetDir = "target/${stdenv.hostPlatform.rust.rustcTargetSpec}/release";
    in ''
      runHook preInstall
      mkdir -p $out/lib/php/extensions
      cp ${targetDir}/libflow_php${stdenv.hostPlatform.extensions.sharedLibrary} $out/lib/php/extensions/flow_php.so
      runHook postInstall
    '';

    doCheck = false;

    meta = with lib; {
      description = "Flow PHP native extension (Rust) - Floe frame-body encoder/decoder for DataFrame Rows";
      license = licenses.mit;
    };
  };
in
pkg // { extensionName = "flow_php"; }
