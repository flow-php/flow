{ php, fetchFromGitHub }:

php.buildPecl {
  pname = "snappy";
  version = "0.2.3";

  src = fetchFromGitHub {
    owner = "kjdev";
    repo = "php-ext-snappy";
    tag = "0.2.3";
    hash = "sha256-W3TJ/bJz1LEPXq8m8YWAYX/2IZoJEpvqzasBiN61hK0=";
    fetchSubmodules = true;
  };

  env.NIX_CXXFLAGS_COMPILE = "-std=c++11";
}
