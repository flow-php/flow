{
  php,
  php-snappy,
  php-lz4,
  php-brotli,
  php-zstd,
}:

let
  myPhp = php.withExtensions (
    { enabled, all }:
    with all;
    enabled
    ++ [
      bcmath
      dom
      mbstring
      (php-brotli.override { inherit php; })
      (php-lz4.override { inherit php; })
      (php-snappy.override { inherit php; })
      (php-zstd.override { inherit php; })
      xmlreader
      xmlwriter
      zlib
      pcov
    ]
  );
in
myPhp.buildEnv {
  extraConfig = ''
    date.timezone = UTC
    max_execution_time = 1800
    max_input_time = 3600
    max_input_nesting_level = 64
    memory_limit = -1
    post_max_size = 200M
    upload_max_filesize = 150M
    file_uploads = On
    max_file_uploads = 20
    short_open_tag = off
  '';
}
