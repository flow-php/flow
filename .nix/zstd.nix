{ buildPecl, fetchgit }:

buildPecl {
  pname = "zstd";
  version = "0.14.0";

  src = fetchgit {
    url = "https://github.com/kjdev/php-ext-zstd.git";
    rev = "0.14.0";
    sha256 = "sha256-oIbvaLYQ6Tp20Y/UEN7i1dtMnxGdMNcIjv6xRCyVYdE=";
    fetchSubmodules = true;
  };
}