## Prepare Project:

To run tests locally, please make sure you have [docker](https://www.docker.com/) up and running.
You also need [PHP 8.2](https://www.php.net/) and [composer](https://getcomposer.org/) to be available from your CLI.
Even though we are supporting 3 PHP versions at the time, we are using the lowest supported one for development, currently it's PHP 8.2.


**HEADS UP** - instead of using php installed on your host machine, consider using Nix Shell.
You can find detailed instructions how to use Nix in the [Nix Development Environment](/documentation/contributing/nix.md) section.

```shell
cp compose.yml.dist compose.yml
just install
docker compose up -d
```

Service and test env config (database URLs, S3/Redis/Azurite/OTel endpoints, the filesystem cache dir) lives in the
committed `.env.dist`, loaded for both PHPUnit and phpbench by the root `bootstrap.php`. The `.dist` defaults match the
`compose.yml.dist` ports, so a standard setup needs no extra step. If you shift service ports in your own `compose.yml`,
copy `.env.dist` to `.env` (gitignored) and set the shifted values there - `.env` overrides `.env.dist` per-variable:

```shell
cp .env.dist .env
```

> If you keep a personal gitignored `phpunit.xml`, regenerate it from `phpunit.xml.dist` after pulling this change: the
> new `.dist` uses `bootstrap="bootstrap.php"` and no longer carries the `<php><env>` block (env now comes from `.env.dist`
> / `.env`). A stale `phpunit.xml` keeps the old bootstrap and env, bypassing the shared config.

For the code coverage, please install [pcov](https://pecl.php.net/package/pcov).

Pcov extension is not mandatory, and tests are going to pass without it; however, you won't be able to run mutation tests.

Run `just --list` to see every available task.

## Run Test Suite

```shell
just test
```

Above command will run all tests, including those that require custom extensions.
To run a single test or a specific phpunit testsuite, forward arguments to `just test`:

```shell
just test --filter=my_test_method
just test --testsuite=lib-parquet-unit
```

`tools/phpunit/vendor/bin/phpunit --list-suites` shows every testsuite defined in `phpunit.xml.dist`.

## Run Lint & Static Analyze

```shell
just lint
just analyze
```

`just lint` runs Mago (format check + lint) and validates the monorepo configuration.
`just analyze` runs Mago static analysis.

**Important** static analyze **MUST** be executed at the lowest supported PHP version
and with dependencies locked by `composer.lock`.
Please make sure to use PHP 8.2 and that you used the `just install` command first.

## Fixing Coding Standards

Before committing your code, please make sure that your code is following our coding standards.

```shell
just fix
```

This command will automatically fix all coding standards issues in your code.
If you want to first check what needs to be fixed, you can use:

```shell
just lint-mago
```

## Test everything

This command will execute exactly the same tests as we run at GitHub Actions before a PR can get merged.
If it passes locally, you are good to open a pull request.

```shell
just build
```

## Building Documentation

Since documentation for DSL and our entire API is automatically generated, you can build it by running:

```shell
just docs
just docs-api
```

`just docs` must be executed after any adjustments to `functions.php` files (DSL).

## Building PHAR

```shell
just phar
./build/flow.phar --version
```

## Building Docker Image

In order to build docker image and load it to local registry please use:

```shell
just docker
```

Usage:

```shell
docker run -v $(pwd):/flow-workspace -it flow-php/flow:latest --version
```
