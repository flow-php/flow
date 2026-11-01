# Nix - Development Environment

[TOC]

Nix is probably the easiest way to set up the development environment.

Before you start, make sure that Nix is installed.
If it is not installed, follow the official documentation to install it.

[Nix installation instructions](https://nixos.org/download/)

After you install Nix, go to the project folder and run this command to start your development environment:

```bash
nix-shell
```

> The first time that you use Nix, the download of all the dependencies takes some time.

> For full isolation, use the `nix-shell --pure` command. Nix then isolates your development environment from the system.

This command gives you all the tools and dependencies of the project in a new shell.

By default, the shell uses [Starship](https://starship.rs/) for the bash prompt.
To override it, create `/.nix/shell/starship.toml` from the `/.nix/shell/starship.toml.dist` file.

After a change, run `nix-shell` again to apply it.

To use the php version from nix in your IDE, start a nix shell with `nix-shell`
and type:

```shell
type php
```

This command shows the path to the php version of the nix shell.
The output looks like this:

```shell
php is /nix/store/p2m5bamh01ncpwjxscdl11p2m9xy8aq6-php-with-extensions-8.x.y/bin/php
```

## php.ini

The nix shell has a predefined php.ini. If it is not sufficient for you, create your own php.ini file at this path:

`./.nix/php/lib/php.ini`

If that file is not there, the shell uses the default php.ini.dist from the same location.

## Pcov

- `pcov`: necessary for code coverage

To start the nix shell without the pcov extension, use the `--arg with-pcov false` flag:

```shell

nix-shell --arg with-pcov false
```

To configure pcov, create a file `./.nix/php/lib/pcov.ini` with your pcov configuration.

## Xdebug

- `xdebug`: necessary for debugging

To install the xdebug extension, start the nix shell with the `--arg with-xdebug true` flag:

```shell
nix-shell --arg with-xdebug true
```

To configure xdebug, create a file `./.nix/php/lib/xdebug.ini` with your xdebug configuration.

## Blackfire

- `blackfire`: necessary for profiling

To install the blackfire extension, start the nix shell with the `--arg with-blackfire true` flag:

```shell
nix-shell --arg with-blackfire true
```

To configure blackfire, create a file `./.nix/php/lib/blackfire.ini` with your blackfire configuration.

## Benchmarking PHP

The default shell runs PHP with a development configuration. OPcache is off for the CLI, so the JIT is also off.
`zend.assertions` is `1`. pcov is loaded. To time Flow with a production configuration:

```shell
nix-shell --arg with-php-benchmark true
```

The flag removes pcov, xdebug and blackfire. If you combine the flag with one of them, the shell fails at evaluation.
The flag appends `./.nix/php/lib/benchmark.ini.dist` to `php.ini`:

```ini
opcache.enable=1
opcache.enable_cli=1
opcache.jit=tracing
opcache.jit_buffer_size=128M
opcache.memory_consumption=256
opcache.interned_strings_buffer=32
opcache.max_accelerated_files=20000
opcache.validate_timestamps=1
zend.assertions=-1
```

On Linux, the flag also sets `opcache.huge_code_pages=1`. To override a value, create `./.nix/php/lib/benchmark.ini`.

## Changing PHP Versions

To change the PHP version, start the nix shell with the `--arg php-version 8.3` flag:

```shell
nix-shell --arg php-version 8.3
```

> Usually, do not change the PHP version. Development must always use the lowest supported PHP version.
>
> This feature is mostly for tests of new integrations
> or of the lowest and highest versions of dependencies.

## Optional Tool Groups

The nix shell has optional tool groups. You enable each group with a flag.
The default shell stays small, and you can still get the special tools.

### WASM Build Tools

```shell
nix-shell --arg with-wasm true
```

For more information, read the [WASM Interactive Playground Build](/documentation/contributing/wasm.md) page.

### Terraform

To add Terraform and Node.js for infrastructure development:

```shell
nix-shell --arg with-terraform true
```

### pg-query-ext Extension (C)

```shell
nix-shell --arg with-pg-query-ext false --arg with-c true
```

For more information, read the [C pg-query Extension Development](/documentation/contributing/c.md) page.

### arrow-ext and flow-php-ext Extensions (Rust)

```shell
nix-shell --arg with-rust true
```

`with-arrow-ext` and `with-flow-php-ext` both default to `!with-rust`. This one flag therefore turns off the prebuilt
extensions for both.

For more information, read the [Rust Extension Development](/documentation/contributing/rust.md) page.

### Protobuf / gRPC Code Generation (protoc)

This flag adds `protoc` and `grpc_php_plugin` to the shell. You need them to generate the OTLP protobuf and gRPC PHP
classes again with `just gen-protobuf-otlp`.

```shell
nix-shell --arg with-protoc true
```

The default shell has the `ext-protobuf` and `ext-grpc` PHP extensions (`with-protobuf` / `with-grpc`, both default
`true`). To remove them, give `false`.

### Reference Engines (DuckDB, Polars, Spark)

Each engine has its own flag. Enable only the engines that a comparison needs:

```shell
nix-shell --arg with-duckdb true --arg with-polars true --arg with-spark true --arg with-php-benchmark true
```

- `with-duckdb` adds the `duckdb` CLI.
- `with-polars` adds a `python3` with `polars` and `pyarrow`.
- `with-spark` adds `spark-submit` with its JDK, without R.

The scripts and the measurement rules are on the
[Benchmarks page](/documentation/contributing/benchmarks.md#reference-engines).

### Combining Multiple Options

You can combine many arguments:

```shell
nix-shell --arg with-terraform true --arg with-wasm true
```

## Local Webserver

To run the local webserver for the development of the Flow website, use the Symfony CLI app. The nix shell contains
it.

```shell
cd web/landing
symfony proxy:start
symfony server:start -d 
```

For more information, read:

- [How to use .wip domain for development](https://symfony.com/doc/current/setup/symfony_server.html#setting-up-the-local-proxy)
