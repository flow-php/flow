{
    php,
    fetchurl,
    php-snappy,
    php-lz4,
    php-brotli,
    php-zstd,
    php-pg-query-ext,
    php-arrow-ext,
    php-flow-php-ext,
    with-pcov ? true,
    with-xdebug ? false,
    with-blackfire ? false,
    with-pg-query-ext ? false,
    with-arrow-ext ? false,
    with-flow-php-ext ? false,
    with-grpc ? false,
    with-protobuf ? true
}:

let
    flowPHP = php.withExtensions (
    { enabled, all }:
    with all;
    enabled
        ++ [
            apcu
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
        ]
        ++ (if with-xdebug then [xdebug] else [])
        ++ (if with-pcov then [pcov] else [])
        ++ (if with-blackfire then [blackfire] else [])
        ++ (if with-pg-query-ext then [(php-pg-query-ext.override { inherit php; })] else [])
        ++ (if with-arrow-ext then [(php-arrow-ext.override { inherit php; })] else [])
        ++ (if with-flow-php-ext then [(php-flow-php-ext.override { inherit php; })] else [])
        ++ (if with-grpc then [grpc] else [])
        ++ (if with-protobuf then [
            (protobuf.overrideAttrs (old: {
                version = "5.35.0";
                src = fetchurl {
                    url = "https://pecl.php.net/get/protobuf-5.35.0.tgz";
                    sha256 = "1wk5q2fd7wlb2qs41ikdbdhpf4248i86fsphg7gnq97zi88aar7m";
                };
            }))
        ] else [])
    );
in
flowPHP.buildEnv {
    extraConfig = ""
    + (
        if builtins.pathExists ./../../php/lib/php.ini
            then builtins.readFile ./../../php/lib/php.ini
            else builtins.readFile ./../../php/lib/php.ini.dist
    )
    + "\n"
    + (
        if with-xdebug
        then
            if builtins.pathExists ./../../php/lib/xdebug.ini
            then builtins.readFile ./../../php/lib/xdebug.ini
            else builtins.readFile ./../../php/lib/xdebug.ini.dist
        else ""
      )
    + "\n"
    + (
        if with-blackfire
        then
            if builtins.pathExists ./../../php/lib/blackfire.ini
                then builtins.readFile ./../../php/lib/blackfire.ini
                else builtins.readFile ./../../php/lib/blackfire.ini.dist
        else ""
      )
    + "\n"
    + (
        if with-pcov
        then
            if builtins.pathExists ./../../php/lib/pcov.ini
                then builtins.readFile ./../../php/lib/pcov.ini
                else builtins.readFile ./../../php/lib/pcov.ini.dist
        else ""
      );
}
