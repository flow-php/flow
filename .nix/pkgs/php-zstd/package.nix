{ php, fetchFromGitHub }:

php.buildPecl {
  pname = "zstd";
  version = "0.14.0";

  src = fetchFromGitHub {
    owner = "kjdev";
    repo = "php-ext-zstd";
    tag = "0.14.0";
    hash = "sha256-oIbvaLYQ6Tp20Y/UEN7i1dtMnxGdMNcIjv6xRCyVYdE=";
    fetchSubmodules = true;
  };
}
