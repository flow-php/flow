{
  php,
  fetchFromGitHub,
}:

php.buildPecl {
  pname = "brotli";
  version = "0.21.1";

  src = fetchFromGitHub {
    owner = "kjdev";
    repo = "php-ext-brotli";
    tag = "0.21.1";
    hash = "sha256-Cs7LT8leCXN2CLU1gZGFfZrvp4u4C77SbBSARt8bmaE=";
    fetchSubmodules = true;
  };
}
