# Rust - Arrow Extension Development

[TOC]

This document describes how to develop the `arrow-ext` PHP extension, which is written in Rust
using the [ext-php-rs](https://github.com/extphprs/ext-php-rs) framework.

## Overview

The `arrow-ext` extension provides a high-performance Parquet reader and writer for PHP,
powered by the [Apache Arrow](https://arrow.apache.org/) Rust ecosystem. It exposes
`Flow\Arrow\Parquet\Reader` and `Flow\Arrow\Parquet\Writer` classes to PHP.

For usage documentation, see [Arrow Extension](/documentation/components/extensions/arrow-ext.md).

## Development Setup

```bash
nix-shell --arg with-arrow-ext false --arg with-rust true
```

This provides the Rust toolchain, clang, libclang, and PHP dev headers for building the extension
from source. After `make build`, the freshly compiled extension is loaded by PHP automatically.

## Project Structure

```
src/extension/arrow-ext/
├── Cargo.toml              # Rust dependencies and build config
├── Makefile                # Build orchestration
├── src/                    # Rust source code
│   ├── lib.rs              # Extension entry point, module registration
│   ├── parquet/            # Parquet reader, writer, type conversion
│   └── stream/             # PHP stream adapters (Read/Write traits)
├── php/                    # PHP stubs for static analysis (used when extension is not loaded)
│   └── Flow/Arrow/
├── tests/
│   ├── phpt/               # PHPT test files
│   └── fixtures/           # Test parquet files
└── ext/
    └── config.m4           # PIE compatibility
```

## Commands

Build the extension:

```bash
nix-shell --arg with-arrow-ext false --arg with-rust true --run "cd src/extension/arrow-ext && make build"
```

Run PHPT tests:

```bash
nix-shell --arg with-arrow-ext false --arg with-rust true --run "cd src/extension/arrow-ext && make test"
```

Build and run PHPT tests:

```bash
nix-shell --arg with-arrow-ext false --arg with-rust true --run "cd src/extension/arrow-ext && make build && make test"
```

Run PHP-side parquet tests (uses the pre-built extension):

```bash
nix-shell --run "just test --testsuite=lib-parquet-integration"
nix-shell --run "just test --testsuite=adapter-parquet-integration"
```

Clean build artifacts:

```bash
nix-shell --arg with-arrow-ext false --arg with-rust true --run "cd src/extension/arrow-ext && make clean"
```

## Make Targets

| Target    | Description                        |
|-----------|------------------------------------|
| `build`   | Build the extension (cargo + copy) |
| `test`    | Run PHPT tests                     |
| `install` | Install to system PHP              |
| `clean`   | Remove build artifacts             |
| `rebuild` | Full clean + build                 |
