# Project Documentation

[TOC]

The project has documentation on different levels:

- Markdown files in the `documentation` directory
- API documentation that PHPDoc generates in the `/web/landing/build/documentation/` directory
- Usage examples in the `web/landing/content/examples` directory, in groups by topic

# Project structure

This project is a monorepo. One repository contains many packages.
The structure of the project is:

```
├── bin
├── build
├── documentation
├── phpdoc
├── src
│   ├── adapter
│   ├── bridge
│   ├── cli
│   ├── core
│   ├── extension
│   ├── lib
│   └── tools
├── tools
├── var
├── vendor
└── web
    └── landing
```

- `bin` contains the executable scripts for development.
- `build` contains the build artifact. At this time, this is only the PHAR file. To build the PHAR file, run
  `just phar`.
- `documentation` contains the documentation files of the full project and of each package. The documentation
  website is generated from these markdown files, which are converted into html.
- `phpdoc` contains the phpDocumentor XML configuration. `just docs-api` generates
  the API documentation into `web/landing/build/documentation/api/`.
- `src` contains the source code of the project, in these packages:
    - `adapter` packages give implementations of the DataFrame building blocks, for example `Extractor`, `Loader` and
      `Transformer`.
    - `bridge` contains bridges that connect flow libs with other libraries and frameworks.
    - `cli` contains the command line interface application.
    - `core` contains the core of the project. It holds the full DataFrame.
    - `extension` contains PHP extensions in other languages (Rust, C) for the parts where performance is critical.
    - `lib` contains standalone libraries that you can use without the rest of the project, for example
      `doctrine-dbal-bulk` and `parquet`.
    - `tools` contains tools for development.
- `tools` contains tools for development, for example `mago` and `phpunit`. They are outside of the project
  dependencies, so they do not add classes to the project autoloader.
- `var` contains temporary files, for example cache and logs.
- `vendor` contains the dependencies of the project. Composer manages them.
- `web`
    - `landing` contains the landing page of the project. It is a symfony application. The build dumps it to static
      HTML files, and GitHub Pages serves them. It has its own composer.json with the dependencies for the landing page
      and its commands.

## Monorepo packages

All packages have almost the same structure:

```
src/core
└── etl
    ├── CONTRIBUTING.md
    ├── LICENSE
    ├── README.md
    ├── composer.json
    ├── src
    │   └── Flow
    └── tests
        └── Flow
```

Tests are usually in two groups:

- `Unit`: `src/core/etl/tests/Flow/ETL/Tests/Unit`
- `Integration`: `src/core/etl/tests/Flow/ETL/Tests/Integration`

Some packages also have test helpers on that level, for example:

- `Doubles`: mocks, stubs and fakes that many tests use

### Packages Dependencies

Keep the number of dependencies as small as possible. When you add a dependency, make its version constraint as wide
as possible, so that our constraints do not block other projects.

Packages from this monorepo can depend on each other, with strict rules:

- `lib`: libraries can depend only on other `lib` packages, never on anything else.
- `adapter`: adapters can depend on `lib` / `bridge`, and they always depend on `core`. Adapters must not depend on
  `cli`.
- `bridge`: bridges can depend only on `lib`.
- `cli`: CLI can depend on `core`, `lib`, `adapter` and `bridge`. `cli` always depends on `core`.
- `core`: core can depend on `lib` or `bridge`. It must never depend on `adapter` or `cli`.

These rules also apply to namespaces. For example, the `CSV` adapter cannot depend on the `Parquet` adapter.

# PHP Versions

This project supports only the latest three PHP versions, at this time 8.3, 8.4 and 8.5.
Development uses the lowest supported version, at this time PHP 8.3.
The tests run on all three versions, so you can use any of them for development.

# Tools

The project uses different tools for development, tests and builds.
Each tool has a `just` recipe. Run `just --list` to see all available tasks.

- `just lint` runs all linters: `lint-mago` (Mago format check and lint), `lint-monorepo`
  (monorepo validation) and `lint-actions` (GitHub Actions audit).
- `just analyze` runs `mago guard` and the Mago static analysis.
- `just fix` runs Mago to fix coding standard and lint issues in the code.
- `just test` runs all tests in the project. To run fewer tests, give phpunit arguments, for example
  `just test --testsuite=lib-parquet-unit` or `just test --filter=my_test_method`. To see the available testsuites,
  run `tools/phpunit/vendor/bin/phpunit --list-suites`.
- `just test-website` runs the tests for the website.
- `just test-mutation` runs the mutation tests, which measure the quality of the tests.
- `just build` runs the full pipeline: lint + analyze + tests + mutation tests.

# Coding Standards

The full project is object oriented code that follows clean code principles.
Mago defines the coding standards and enforces them.

Each package has a DSL (Domain Specific Language), a functional API that is easy to use.
All DSL functions (usually in `functions.php` files) follow the same coding standards as the other PHP code.
DSL function names and arguments use snake_case. The only other place that uses snake_case is the tests, for test
method names.

To make sure that the full project follows the coding standards, run these commands:

```
just fix
just lint
just analyze
```

Commit your changes only after all three commands pass.

# Testing

- The tests in the project are in two groups:
    - `Unit` tests one behavior in isolation, without dependencies.
    - `Integration` tests one behavior with dependencies, for example a database or external services.
- The test cases of all packages must extend the `\Flow\ETL\Tests\FlowTestCase` class. The only exceptions are the
  `lib` and `bridge` packages, which can use their own test cases.
- Each test method tests only one scenario. To test one behavior with different input data, use the
  `PHPUnit\Framework\Attributes\TestWith` or `PHPUnit\Framework\Attributes\DataProvider` attributes.
