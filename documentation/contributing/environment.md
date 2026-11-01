## Prepare Project:

To run the tests locally, you need a running [docker](https://www.docker.com/).
You also need [PHP 8.3](https://www.php.net/) and [composer](https://getcomposer.org/) in your CLI.
The minimum PHP version is 8.3, and 8.4 and 8.5 are also supported. Development uses the minimum version, PHP 8.3.


Instead of the PHP on your host machine, you can use Nix Shell.
The [Nix Development Environment](/documentation/contributing/nix.md) section tells you how to use Nix.

```shell
cp compose.yml.dist compose.yml
just install
docker compose up -d
```

The committed `.env.dist` file contains the configuration for the services and the tests: database URLs, S3, Azurite
and OTel endpoints, and the filesystem cache directory. The root `bootstrap.php` loads it for PHPUnit and for phpbench.
The `.dist` defaults match the `compose.yml.dist` ports, so a standard setup needs no extra step. If you change the
service ports in your own `compose.yml`, copy `.env.dist` to `.env` and set the changed values there. Git ignores
`.env`. Each variable in `.env` overrides the same variable in `.env.dist`:

```shell
cp .env.dist .env
```

> Upgrading from an older checkout: the local S3 service is now [RustFS](https://rustfs.com/), not MinIO. MinIO
> deleted its community images from Docker Hub. The S3 credentials changed from `minioadmin` to
> `flowphpaccess01` / `flowphpsecret01`. Update both files. If you do not, each S3 request fails with
> `SignatureDoesNotMatch`:
>
> ```shell
> docker compose down -v
> rm -rf var/minio
> cp compose.yml.dist compose.yml
> cp .env.dist .env          # or update S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY in your existing .env
> docker compose up -d
> ```

> If you keep a personal `phpunit.xml` (git ignores it), make it again from `phpunit.xml.dist` after you pull this
> change. The new `.dist` uses `bootstrap="bootstrap.php"` and does not have the `<php><env>` block. The environment
> now comes from `.env.dist` / `.env`. An old `phpunit.xml` keeps the old bootstrap and environment, and it does not
> use the shared configuration.

For code coverage, install [pcov](https://pecl.php.net/package/pcov).

The tests pass without the pcov extension. Without it, you cannot run mutation tests.

Run `just --list` to see all available tasks.

## Run Test Suite

```shell
just test
```

This command runs all tests, also the tests that need custom extensions.
To run one test or one phpunit testsuite, give the arguments to `just test`:

```shell
just test --filter=my_test_method
just test --testsuite=lib-parquet-unit
```

`tools/phpunit/vendor/bin/phpunit --list-suites` shows all testsuites in `phpunit.xml.dist`.

## Run Lint & Static Analyze

```shell
just lint
just analyze
```

`just lint` runs `lint-mago` (Mago format check and lint), `lint-monorepo` (monorepo validation)
and `lint-actions` (GitHub Actions audit).
`just analyze` runs `mago guard` and the Mago static analysis.

Run the static analysis on the lowest supported PHP version, with the dependencies that `composer.lock` locks.
Use PHP 8.3, and run the `just install` command first.

## Fixing Coding Standards

Before you commit your code, make sure that it follows the coding standards.

```shell
just fix
```

This command fixes all coding standard issues in your code.
To see the issues first, without a fix, use:

```shell
just lint-mago
```

## Test everything

This command runs the same tests that GitHub Actions runs before a pull request can merge.
If it passes locally, you can open a pull request.

```shell
just build
```

## Building Documentation

The documentation for the DSL and the full API is generated. To build it, run:

```shell
just docs
just docs-api
```

Run `just docs` after each change to a `functions.php` file (DSL).

## Building PHAR

```shell
just phar
./build/flow.phar --version
```

## Building Docker Image

To build the docker image and load it into the local registry, use:

```shell
just docker
```

Usage:

```shell
docker run -v $(pwd):/flow-workspace -it flow-php/flow:latest --version
```
