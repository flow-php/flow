{
    php,
    php-snappy,
    php-lz4,
    php-brotli,
    php-zstd,
    php-pg-query-ext,
    php-arrow-ext,
    with-pcov ? true,
    with-xdebug ? false,
    with-blackfire ? false,
    with-pg-query-ext ? false,
    with-arrow-ext ? false,
    with-grpc ? false,
    with-protobuf ? true
}:

let
    flowPHP = php.withExtensions (
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
        ]
        ++ (if with-xdebug then [xdebug] else [])
        ++ (if with-pcov then [pcov] else [])
        ++ (if with-blackfire then [blackfire] else [])
        ++ (if with-pg-query-ext then [(php-pg-query-ext.override { inherit php; })] else [])
        ++ (if with-arrow-ext then [(php-arrow-ext.override { inherit php; })] else [])
        ++ (if with-grpc then [grpc] else [])
        ++ (if with-protobuf then [protobuf] else [])
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
