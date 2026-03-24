{
  php,
  lib,
  stdenv,
  rustPlatform,
  clang,
  llvmPackages,
}:

let
  extSrc = builtins.path {
    path = ../../../src/extension/arrow-ext;
    name = "arrow-ext-src";
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
    pname = "php-arrow-ext";
    version = "0.1.0";

    src = extSrc;

    cargoLock.lockFile = ../../../src/extension/arrow-ext/Cargo.lock;

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
    };

    installPhase = let
      targetDir = "target/${stdenv.hostPlatform.rust.rustcTargetSpec}/release";
    in ''
      runHook preInstall
      mkdir -p $out/lib/php/extensions
      cp ${targetDir}/libarrow${stdenv.hostPlatform.extensions.sharedLibrary} $out/lib/php/extensions/arrow.so
      runHook postInstall
    '';

    doCheck = false;

    meta = with lib; {
      description = "Apache Arrow PHP extension powered by Rust";
      license = licenses.mit;
    };
  };
in
pkg // { extensionName = "arrow"; }
