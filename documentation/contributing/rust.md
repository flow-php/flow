# Rust - Extension Development

[TOC]

This document describes how to develop the two Rust PHP extensions in this monorepo, both written with the
[ext-php-rs](https://github.com/extphprs/ext-php-rs) framework.

| Extension      | Package                 | What it provides                                                                                                                                                             |
|----------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `arrow-ext`    | `flow-php/arrow-ext`    | Parquet reader and writer powered by the [Apache Arrow](https://arrow.apache.org/) Rust ecosystem, exposed as `Flow\Parquet\Engine\RustParquetEngine`                        |
| `flow-php-ext` | `flow-php/flow-php-ext` | The native column backend (`Flow\ETL\Values\Column\RustBackend`, `RustColumn`, `RustColumnBuilder` over Apache Arrow arrays) and the Rust CSV / JSON / Parquet sources and encoders |

Both are optional. The pure-PHP implementations in `flow-php/etl` and `flow-php/parquet` remain the behaviour
reference; every contract has a `Php*`, a `Rust*` and an `Adaptive*` class that picks the Rust one when the extension
is loaded.

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
│   ├── interfaces.rs       # Flow\Parquet\{ParquetEngine, ParquetFileReader, ParquetFileWriter}, registered at MINIT
│   ├── parquet/            # RustParquetEngine, readers, writer, canonical Arrow types, Arrow C Data batches
│   ├── thrift.rs           # Footer bytes as the PHP ThriftModel objects
│   ├── render.rs           # Parquet refusals as the Flow\Parquet exceptions
│   ├── php.rs              # PHP engine helpers (class entries, hashtables, calls)
│   ├── values.rs           # PHP values through the engine's C date API
│   ├── alloc.rs            # Counting global allocator
│   └── exception.rs        # Exception mapping
├── php/                    # PHP stubs for static analysis (used when extension is not loaded)
│   └── Flow/
│       ├── Arrow/
│       └── Parquet/Engine/
├── tests/
│   └── phpt/               # PHPT test files
└── ext/
    └── config.m4           # PIE compatibility

src/extension/flow-php-ext/
├── Cargo.toml              # Rust dependencies and build config
├── build.rs                # Build script
├── Makefile                # Build orchestration
├── src/                    # Rust source code
│   ├── lib.rs              # Extension entry point, module registration
│   ├── interfaces.rs       # the package interfaces the Rust classes implement, registered at MINIT
│   ├── iterator.rs         # RustIterator, the iterator the Rust sources return
│   ├── backend.rs          # RustBackend
│   ├── column.rs           # RustColumn
│   ├── builder.rs          # RustColumnBuilder: native cast lanes, PHP lane for the rest
│   ├── physical.rs         # Arrow rows as physical and logical zvals
│   ├── plan.rs             # Per-type plan, cached by Type object and type JSON
│   ├── render.rs           # flow-batch-frame refusals as the PHP messages
│   ├── cast.rs             # Value casting
│   ├── json_check.rs       # JSON validation shared by casting and CSV inference
│   ├── batch_columns.rs    # BatchColumns: one native builder per definition, shared by the CSV and JSON sources
│   ├── source.rs           # Shared stream loop of the Rust open sources
│   ├── csv/                # RustCSVOpenSource, RustCSVEncoder, schema-inference fold
│   ├── json/               # RustJsonOpenSource, RustJsonEncoder, JSON values into columns
│   ├── text/               # The PHP side of the text writers: Rows batches, DateTimeZone transitions
│   ├── parquet/            # RustParquetOpenSource, RustParquetFileWriter
│   ├── arrow_c.rs          # RustColumnsBatch and the PHP side of the Arrow C Data hand-off (arrow-ext)
│   ├── ctx.rs              # Request-scoped context and engine helpers
│   ├── globals.rs          # Module globals, RINIT/RSHUTDOWN
│   ├── alloc.rs            # Counting global allocator (allocatedBytes())
│   ├── values.rs           # Zval <-> PHP value helpers
│   └── exception.rs        # Exception mapping
├── crates/                 # The cargo workspace members: pure Rust, no PHP types, every #[test] lives here
│   ├── flow-batch-frame/   # Column buffers, builders, BATCH frame bodies, kinds and value shapes, column keys
│   ├── flow-column-text/   # Column values as the writers render them: floats, dates, JSON strings, uuids
│   ├── flow-json-records/  # JSON lines / array framing and member-to-column mapping
│   ├── flow-arrow-c/       # Batches across the Arrow C Data Interface, imported bytes counted
│   ├── flow-csv-tokenizer/ # CSV records and fields as php_fgetcsv() produces them
│   └── flow-numeric-string/ # PHP's is_numeric() grammar
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

| Target    | Description                                      |
|-----------|--------------------------------------------------|
| `build`   | Build the extension (cargo + copy)               |
| `test`    | Build, then run PHPT tests                       |
| `install` | Copy the built module into PHP's `extension_dir` |
| `clean`   | Remove build artifacts                           |
| `rebuild` | Full clean + build                               |

The two PHPT runners are the same shell loop: each test runs with `php -d extension=<the built .so>` (not `php -n`, so
the ambient `php.ini` and its extensions load alongside), `--SKIPIF--` blocks are honoured and the skipped count is
reported, and the output is compared exactly (`--EXPECTF--` is accepted but `%s` is not expanded). `flow-php-ext`
additionally takes `ARROW_SO=<path>` to pre-load arrow-ext, so the Parquet ETL phpts run instead of skipping:
`make test ARROW_SO=../arrow-ext/ext/modules/arrow.so`.

## Rebuilding after a source change

`.nix/pkgs/php-arrow-ext` and `.nix/pkgs/php-flow-php-ext` build their extension from the local repository source, so a
shell you entered before editing any `.rs` still embeds the **previous** build. Running `just test` in a stale shell
produces failures that are artifacts of the old binary, not of your change.

Re-enter `nix-shell` to rebuild the derivation, or build and test the extension directly:

```bash
nix-shell --arg with-rust true --run "cd src/extension/flow-php-ext && make build && make test"
```

This covers every crate under `crates/`, which the flow-php-ext build compiles from the same workspace.

## Extension ABI

Each extension registers an `int` constant with the version of the contract its PHP package expects. The PHP package
holds the same number:

| Extension | Constant (Rust `lib.rs`) | PHP side                                    |
|-----------|--------------------------|---------------------------------------------|
| flow_php  | `FLOW_PHP_ABI`           | `Flow\ETL\Foundation\FlowPhpExtension::ABI` |
| arrow     | `FLOW_ARROW_ABI`         | `Flow\Parquet\Engine\ArrowExtension::ABI`   |

Bump both, in one change, whenever the contract changes: a class or interface the extension registers, or the
schema, options or values that cross it. Every `Adaptive*` pick calls `available()`: an extension that is loaded with
another ABI, or with none, is refused with a `RuntimeException` naming the extension and its version. phpt `099`
(flow_php) and `054` (arrow) pin that the two numbers are equal.

The PHP interfaces an extension also registers are guarded by `interface_exists(X::class, false)`, so an extension
that does not register one still gets the PHP declaration and fails at the ABI check, not with "Class not found".

## The flow-php-ext workspace

`src/extension/flow-php-ext` is a cargo workspace: the root package is the extension (PHP glue only: ext-php-rs classes,
Zval conversions, exception text), the members under `crates/` are pure Rust with no PHP types. Every `#[test]` lives in
a member, so the test binaries link without a PHP engine; a member that reaches a Zend symbol fails to link on Linux,
which CI runs for every push. Each member returns its refusals as data (`Error`, `Refusal`, `Malformed`); `flow_php`
renders each into the PHP message. `flow-batch-frame` holds Flow's batch layout: one column's buffers in the canonical
form of `Column::encode()` and back, the builders that append rows into it, and the BATCH frame body around them.

```bash
nix-shell --arg with-rust true --run "cd src/extension/flow-php-ext && cargo test --workspace --features arrow-validate"
nix-shell --arg with-rust true --run "cd src/extension/flow-php-ext && cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings"
```

`arrow-validate` turns on `arrow-data/force_validate`, which re-validates every array the builders hand over with
`build_unchecked`; `make test` builds the extension with it, `make build` without. The same feature and the same three
commands (without `--workspace`) apply to `src/extension/arrow-ext`.

arrow-ext does not depend on the workspace. Its `canonical_type()` (`src/parquet/canonical.rs`) must produce the Arrow
types of flow-batch-frame's `kind::data_type()`, pinned by flow-php-ext phpt 091.

## Releasing

Nothing to bump. Untagged builds report `X.(Y+1).0-dev+<commits>.g<sha>` after the tag `X.Y.Z`; the nix packages
report `X.(Y+1).0-dev`, read from the newest `X.Y.Z` tag in `.git` by `shell.nix`. A checkout
without tags fails the nix-shell with a pointer to `git fetch --tags`.
