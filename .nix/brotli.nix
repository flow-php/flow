{ buildPecl, fetchgit }:

buildPecl {
  pname = "brotli";
  version = "0.13.1";

  src = fetchgit {
    url = "https://github.com/kjdev/php-ext-brotli.git";
    rev = "0.13.1";
    sha256 = "sha256-bdnTEEJUPe+VvXjncKbIi4wfnEn9UH7OBTKiUCET+qQ=";
  };
}