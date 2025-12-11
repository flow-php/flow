{ php, fetchFromGitHub }:

php.buildPecl {
  pname = "lz4";
  version = "0.6.0";

  src = fetchFromGitHub {
    owner = "kjdev";
    repo = "php-ext-lz4";
    tag = "0.6.0";
    hash = "sha256-F98nSyQG/je9Sggugb6wJbgHR3DPAG9r4Y4eQSDlntI=";
    fetchSubmodules = true;
  };
}
