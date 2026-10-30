# Nix - Development Environment

[TOC]

Nix is probably the easiest way of setting up the development environment.

Before you start please make sure you have Nix installed. 
If you don't have it installed, you can install it by following official documentation.

[Nix installation instructions](https://nixos.org/download/)

Once you have Nix installed, you can start your development environment
by going to the project folder and running following command:

```bash
nix-shell
```

> If you are using Nix for the first time, it might take a while to download all the dependencies.

> To achieve full isolation, use `nix-shell --pure` command. This way nix will isolate your development environment from the system.

That's all, after running this command you will have all the necessary tools and dependencies.
Nix will create a new shell with all the necessary tools and dependencies for the project.

By default, we're using [Starship](https://starship.rs/) to provide a nice bash prompt.
You can override it by creating `/.nix/shell/starship.toml` based on `/.nix/shell/starship.toml.dist` 
file. 

Once you apply your modification you can run `nix-shell` again to apply changes.

To use the php version from nix inside your IDE please start a nix shell `nix-shell`
and type: 

```shell
type php
```

This should return you path to your php version that is used inside of the nix shell.
It will look like this: 

```shell
php is /nix/store/p2m5bamh01ncpwjxscdl11p2m9xy8aq6-php-with-extensions-8.2.27/bin/php
```

## php.ini

Nix shell comes with predefined php.ini, but if for any reason
it wouldn't be enough for you, you can create your own php.ini file in path:

`./.nix/php/lib/php.ini`

If that file is not present, the default php.ini.dist from the same location will be used.

## Pcov

- `pcov` - required for code coverage

To skip installing pcov extension, you can run nix shell with `--arg with-pcov false` flag:

```shell

nix-shell --arg with-pcov false
```

To configure pcov, you can create a file `./.nix/php/lib/pcov.ini` with your xdebug configuration.

## Xdebug

- `xdebug` - required for debugging

To install xdebug extension, you can run nix shell with `--arg with-xdebug true` flag:

```shell
nix-shell --arg with-xdebug true
```

To configure xdebug, you can create a file `./.nix/php/lib/xdebug.ini` with your xdebug configuration.

## Blackfire

- `blackfire` - required for profiling

To install blackfire extension, you can run nix shell with `--arg with-blackfire true` flag:

```shell
nix-shell --arg with-blackfire true
```

To configure blackfire, you can create a file `./.nix/php/lib/blackfire.ini` with your blackfire configuration.

## Benchmarking PHP

The default shell runs PHP with development settings. OPcache is off for the CLI, which keeps the JIT off.
`zend.assertions` is `1`. pcov is loaded. To time Flow with production settings:

```shell
nix-shell --arg with-php-benchmark true
```

The flag removes pcov, xdebug and blackfire. A shell that combines the flag with one of them fails at evaluation.
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

On Linux the flag also sets `opcache.huge_code_pages=1`. To override a value, create `./.nix/php/lib/benchmark.ini`.

## Changing PHP Versions

To change the PHP version, you can run nix shell with `--arg php-version 8.3` flag:

```shell
nix-shell --arg php-version 8.3
```

> In general, it's not recommended to change the PHP version, as development should always
> be done on the lowest supported PHP version.
>
> This feature is mostly for testing new integrations
> or lowest/highest versions of dependencies.

## Optional Tool Groups

The nix shell supports optional tool groups that can be enabled when needed.
This keeps the default shell lightweight while allowing access to specialized tools.

### WASM Build Tools

```shell
nix-shell --arg with-wasm true
```

See [WASM - Interactive Playground Build](/documentation/contributing/wasm.md) for details.

### Terraform

To include Terraform and Node.js for infrastructure development:

```shell
nix-shell --arg with-terraform true
```

### pg-query-ext Extension (C)

```shell
nix-shell --arg with-pg-query-ext false --arg with-c true
```

See [C - pg-query Extension Development](/documentation/contributing/c.md) for details.

### arrow-ext and flow-php-ext Extensions (Rust)

```shell
nix-shell --arg with-rust true
```

`with-arrow-ext` and `with-flow-php-ext` both default to `!with-rust`, so this single flag already turns the prebuilt
extensions off for both.

See [Rust - Extension Development](/documentation/contributing/rust.md) for details.

### Protobuf / gRPC Code Generation (protoc)

Adds `protoc` and `grpc_php_plugin` to the shell. Required when regenerating OTLP protobuf and gRPC PHP classes via `just gen-protobuf-otlp`.

```shell
nix-shell --arg with-protoc true
```

The `ext-protobuf` and `ext-grpc` PHP extensions are part of the default shell (`with-protobuf` / `with-grpc`, both default `true`); pass `false` to opt out.

### Reference Engines (DuckDB, Polars, Spark)

Each engine has its own flag. Enable only the engines a comparison needs:

```shell
nix-shell --arg with-duckdb true --arg with-polars true --arg with-spark true --arg with-php-benchmark true
```

- `with-duckdb` adds the `duckdb` CLI.
- `with-polars` adds a `python3` with `polars` and `pyarrow`.
- `with-spark` adds `spark-submit` with its JDK, without R.

The scripts and the measurement rules are on the
[Benchmarks page](/documentation/contributing/benchmarks.md#reference-engines).

### Combining Multiple Options

You can combine multiple arguments:

```shell
nix-shell --arg with-terraform true --arg with-wasm true
```

## Local Webserver

To run the local webserver for Flow Website development, please use Symfony CLI app
that is also available in nix shell.

```shell
cd web/landing
symfony proxy:start
symfony server:start -d 
```

You can read more about it here: 

- [How to use .wip domain for development](https://symfony.com/doc/current/setup/symfony_server.html#setting-up-the-local-proxy)
