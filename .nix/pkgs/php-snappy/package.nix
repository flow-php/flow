{ php, fetchFromGitHub }:

php.buildPecl {
  pname = "snappy";
  version = "0.2.1";

  src = fetchFromGitHub {
    owner = "kjdev";
    repo = "php-ext-snappy";
    tag = "0.2.1";
    hash = "sha256-PAKdIcpJKH6d74EulYQepP4XbQvccrj1nEuir47vro4=";
    fetchSubmodules = true;
  };

  env.NIX_CXXFLAGS_COMPILE = "-std=c++11";
}
