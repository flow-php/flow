{
  php,
  lib,
  stdenv,
  fetchFromGitHub,
  protobufc,
}:

let
  libpg_query = stdenv.mkDerivation {
    pname = "libpg_query";
    version = "17-6.1.0";

    src = fetchFromGitHub {
      owner = "pganalyze";
      repo = "libpg_query";
      rev = "17-6.1.0";
      hash = "sha256-UXba2WYyIO7RcFcNZeLL+Q9CwlloMZ5oFfHfL7+j4dU=";
    };

    buildPhase = ''
      make -j$NIX_BUILD_CORES
    '';

    installPhase = ''
      mkdir -p $out/lib $out/include
      cp libpg_query.a $out/lib/
      cp pg_query.h $out/include/
    '';
  };

  extSrc = builtins.path {
    path = ../../../src/extension/pg-query-ext/ext;
    name = "pg-query-ext-src";
    filter = path: type:
      let baseName = baseNameOf path;
      in !(
        # Exclude build artifacts
        baseName == "Makefile" ||
        baseName == "configure" ||
        baseName == "config.h" ||
        baseName == "config.h.in" ||
        baseName == "config.log" ||
        baseName == "config.status" ||
        baseName == "config.nice" ||
        baseName == "configure.ac" ||
        baseName == "libtool" ||
        baseName == "run-tests.php" ||
        baseName == "autom4te.cache" ||
        baseName == "build" ||
        baseName == "modules" ||
        baseName == ".libs" ||
        lib.hasSuffix ".lo" baseName ||
        lib.hasSuffix ".la" baseName ||
        lib.hasSuffix ".dep" baseName ||
        lib.hasSuffix "~" baseName ||
        lib.hasPrefix "Makefile" baseName
      );
  };
in
php.buildPecl {
  pname = "pg_query";
  version = "1.0.0";

  src = extSrc;

  buildInputs = [ protobufc libpg_query ];

  configureFlags = [
    "--with-pg-query=${libpg_query}"
  ];
}
