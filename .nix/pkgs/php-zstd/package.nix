{ php, fetchFromGitHub }:

php.buildPecl {
  pname = "zstd";
  version = "0.19.0";

  src = fetchFromGitHub {
    owner = "kjdev";
    repo = "php-ext-zstd";
    tag = "0.19.0";
    hash = "sha256-wLAfY5DoPNIHPKAGGd+ozIH9NXkb8AmAj4ZjI+YXCeo=";
    fetchSubmodules = true;
  };
}
