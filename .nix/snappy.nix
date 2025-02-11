{ buildPecl, fetchgit }:

buildPecl {
  pname = "snappy";
  version = "0.2.1";

  env.NIX_CXXFLAGS_COMPILE = "-std=c++11";

  src = fetchgit {
    url = "https://github.com/kjdev/php-ext-snappy.git";
    rev = "0.2.1";
    sha256 = "sha256-PAKdIcpJKH6d74EulYQepP4XbQvccrj1nEuir47vro4=";
    fetchSubmodules = true;
  };
}