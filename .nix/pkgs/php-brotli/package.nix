{
  php,
  fetchFromGitHub,
}:

php.buildPecl {
  pname = "brotli";
  version = "0.13.1";

  src = fetchFromGitHub {
    owner = "kjdev";
    repo = "php-ext-brotli";
    tag = "0.13.1";
    hash = "sha256-bdnTEEJUPe+VvXjncKbIi4wfnEn9UH7OBTKiUCET+qQ=";
    fetchSubmodules = true;
  };
}
