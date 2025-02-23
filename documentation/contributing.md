# Contributing

To run tests locally, please make sure you have [docker](https://www.docker.com/) up and running.
You also need [PHP 8.2](https://www.php.net/) and [composer](https://getcomposer.org/) to be available from your CLI.
Even though we are supporting 3 PHP versions at the time, we are using the lowest supported one for development, currently it's PHP 8.2.


## Before you change anything

Please make sure that you are aware of our [Architecture Decision Records](/documentation/adrs.md).
It's mandatory to follow all of them without any exceptions unless explicitly overridden by a new ADR.

## Prepare Project:

```shell
cp compose.yml.dist compose.yml
composer install 
docker compose up -d
```

For the code coverage, please install [pcov](https://pecl.php.net/package/pcov).

Pcov extension is not mandatory, and tests are going to pass without it; however, you won't be able to run mutation tests. 

## Run Test Suite

```shell
composer test
```

Above command will run all tests, including those that require custom extensions.
In case you want to run tests only for a specific part of the project, you can use: 


```shell
composer test:core
composer test:lib:doctrine-dbal-bulk
composer test:lib:parquet
composer test:adapter:csv
composer test:bridge:symfony-http-foundation
```

## Run Static Analyze

```shell
composer static:analyze
```

**Important** static analyze **MUST** be executed at the lowest supported PHP version
and with dependencies locked by `composer.lock`. 
Please make sure to use PHP 8.2 and that you used the `composer install` command first.

## Fixing Coding Standards

Before committing your code, please make sure that your code is following our coding standards.

```shell
composer cs:php:fix
```

This command will automatically fix all coding standards issues in your code.
If you want to first check what needs to be fixed, you can use:


```shell
composer static:analyze:cs-fixer
composer static:analyze:rector
```

## Test everything

This command will execute exactly the same tests as we run at GitHub Actions before a PR can get merged.
If it passes locally, you are good to open a pull request.

```shell
composer build 
```

## Building Documentation

Since documentation for DSL and our entire API is automatically generated, you can build it by running:

```shell
composer build:docs
composer build:docs:api
```

`composer build:docs` must be executed after any adjustments to `functions.php` files (DSL).

## Building PHAR

```shell
composer build:phar
./build/flow.phar --version
```

## Building Docker Image

In order to build docker image and load it to local registry please use: 

```shell
docker buildx build -t flow-php/flow:latest . --progress=plain --load
```

Usage:

```shell
docker run -v $(pwd):/flow-workspace -it flow-php/flow:latest --version
```
