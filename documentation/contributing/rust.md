# Rust - Extension Development

[TOC]

This document describes how to develop the two Rust PHP extensions in this monorepo, both written with the
[ext-php-rs](https://github.com/extphprs/ext-php-rs) framework.

| Extension | Package | What it provides |
|---|---|---|
| `arrow-ext` | `flow-php/arrow-ext` | Parquet reader and writer powered by the [Apache Arrow](https://arrow.apache.org/) Rust ecosystem, exposed as `Flow\Arrow\Parquet\Reader` and `Flow\Arrow\Parquet\Writer` |
| `flow-php-ext` | `flow-php/flow-php-ext` | Native Floe binary frame encoding/decoding and row hydration/casting against a schema |

Both are optional. The pure-PHP implementations in `flow-php/etl` remain the canonical behaviour reference, and Flow
routes to the native code automatically when the extension is loaded.

For usage documentation, see [Arrow Extension](/documentation/components/extensions/arrow-ext.md) and
[Flow PHP Extension](/documentation/components/extensions/flow-php-ext.md).

## Development Setup

```bash
nix-shell --arg with-rust true
```

This provides the Rust toolchain, clang, libclang, and PHP dev headers for building either extension from source.

`with-arrow-ext` and `with-flow-php-ext` both default to `!with-rust`, so `--arg with-rust true` already turns the
prebuilt extensions off - you do not need to pass them yourself. Pass `--arg with-arrow-ext false` or
`--arg with-flow-php-ext false` only to override an explicit `true`; `shell.nix` asserts when either is combined with
`--arg with-rust true`, because a prebuilt extension and a source build would collide.

> [!IMPORTANT]
> `make build` does **not** make PHP pick up the freshly compiled extension. Each Makefile's `test` target loads the
> binary explicitly with `php -d extension=...`. To use a new build from anything else, run `make install`, pass
> `-d extension=` yourself, or re-enter `nix-shell` - see [Rebuilding after a source change](#rebuilding-after-a-source-change).

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

src/extension/flow-php-ext/
├── Cargo.toml              # Rust dependencies and build config
├── build.rs                # Build script
├── Makefile                # Build orchestration
├── src/                    # Rust source code
│   ├── lib.rs              # Extension entry point, module registration
│   ├── encode.rs           # Floe frame body encoder
│   ├── format.rs           # Floe binary format primitives
│   ├── hydrate.rs          # Row hydration against a schema
│   ├── cast.rs             # Value casting
│   ├── plan.rs             # Per-column plan resolved once per schema
│   ├── ctx.rs              # Shared module context
│   ├── values.rs           # Zval <-> PHP value helpers
│   └── exception.rs        # Exception mapping
├── php/                    # PHP stubs for static analysis
│   └── Flow/
├── tests/
│   └── phpt/               # PHPT test files
└── ext/
    └── config.m4           # PIE compatibility
```

## Commands

Substitute `arrow-ext` or `flow-php-ext` for `<extension>`.

Build:

```bash
nix-shell --arg with-rust true --run "cd src/extension/<extension> && make build"
```

Run PHPT tests (`test` depends on `build`, so this rebuilds first):

```bash
nix-shell --arg with-rust true --run "cd src/extension/<extension> && make test"
```

Clean build artifacts:

```bash
nix-shell --arg with-rust true --run "cd src/extension/<extension> && make clean"
```

Run the PHP-side test suites against the prebuilt extension - note these use the **default** shell, not the Rust one:

```bash
nix-shell --run "just test --testsuite=lib-parquet-integration"      # arrow-ext
nix-shell --run "just test --testsuite=adapter-parquet-integration"  # arrow-ext
nix-shell --run "just test --testsuite=etl-unit"                     # flow-php-ext
```

## Make Targets

| Target    | Description                        |
|-----------|------------------------------------|
| `build`   | Build the extension (cargo + copy) |
| `test`    | Build, then run PHPT tests         |
| `install` | Copy the built module into PHP's `extension_dir` |
| `clean`   | Remove build artifacts             |
| `rebuild` | Full clean + build                 |

The two PHPT runners differ in two ways:

- `arrow-ext` runs each test with `php -n`, so `php.ini` is ignored and no other extension is loaded.
  `flow-php-ext` does not, so the ambient extensions load alongside it.
- `flow-php-ext` honours `--SKIPIF--` blocks and reports a skipped count. `arrow-ext` ignores them.

## Rebuilding after a source change

`.nix/pkgs/php-arrow-ext` and `.nix/pkgs/php-flow-php-ext` build their extension from the local repository source, so a
shell you entered before editing any `.rs` still embeds the **previous** build. Running `just test` in a stale shell
produces failures that are artifacts of the old binary, not of your change.

Re-enter `nix-shell` to rebuild the derivation, or build and test the extension directly:

```bash
nix-shell --arg with-rust true --run "cd src/extension/flow-php-ext && make build && make test"
```
