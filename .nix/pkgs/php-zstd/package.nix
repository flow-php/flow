{ php, fetchFromGitHub }:

php.buildPecl {
  pname = "zstd";
  version = "0.15.2";

  src = fetchFromGitHub {
    owner = "kjdev";
    repo = "php-ext-zstd";
    tag = "0.15.2";
    hash = "sha256-AhEEHtETALgYkJxDfRA6/bx6yZKpsfL48MqO505FAFI=";
    fetchSubmodules = true;
  };
}
