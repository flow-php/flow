{ buildPecl, fetchgit }:

buildPecl {
  pname = "lz4";
  version = "0.4.4";

  src = fetchgit {
    url = "https://github.com/kjdev/php-ext-lz4.git";
    rev = "0.4.4";
    sha256 = "sha256-iKgMN77W5iR3jwOwKNwIpuLwkeDkQVTIppEp4fF1oZw=";
    fetchSubmodules = true;
  };
}