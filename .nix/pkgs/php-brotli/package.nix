{
  php,
  fetchFromGitHub,
}:

php.buildPecl {
  pname = "brotli";
  version = "0.18.3";

  src = fetchFromGitHub {
    owner = "kjdev";
    repo = "php-ext-brotli";
    tag = "0.18.3";
    hash = "sha256-kIsQHgiCYcoa5+5wXjA+VCrMj7ZFLunOCOeG5DFD62k=";
    fetchSubmodules = true;
  };
}
