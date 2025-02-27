{ php, fetchFromGitHub }:

php.buildPecl {
  pname = "lz4";
  version = "0.4.4";

  src = fetchFromGitHub {
    owner = "kjdev";
    repo = "php-ext-lz4";
    tag = "0.4.4";
    hash = "sha256-iKgMN77W5iR3jwOwKNwIpuLwkeDkQVTIppEp4fF1oZw=";
    fetchSubmodules = true;
  };
}
