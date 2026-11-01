# Rust - Extension Development

[TOC]

This page tells you how to develop the two Rust PHP extensions in this monorepo. Both use the
[ext-php-rs](https://github.com/extphprs/ext-php-rs) framework.

| Extension      | Package                 | What it provides                                                                                                                                                                    |
|----------------|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `arrow-ext`    | `flow-php/arrow-ext`    | Parquet reader and writer powered by the [Apache Arrow](https://arrow.apache.org/) Rust ecosystem, exposed as `Flow\Parquet\Engine\RustParquetEngine`                               |
| `flow-php-ext` | `flow-php/flow-php-ext` | The native column backend (`Flow\ETL\Values\Column\RustBackend`, `RustColumn`, `RustColumnBuilder` over Apache Arrow arrays) and the Rust CSV / JSON / Parquet sources and encoders |

Both are optional. The pure-PHP implementations in `flow-php/etl` and `flow-php/parquet` stay the reference for the
behaviour. Each contract has a `Php*`, a `Rust*` and an `Adaptive*` class. If the extension is loaded, the
`Adaptive*` class picks the Rust class.

For the usage documentation, read [Arrow Extension](/documentation/components/extensions/arrow-ext.md) and
[Flow PHP Extension](/documentation/components/extensions/flow-php-ext.md).

## Development Setup

```bash
nix-shell --arg with-rust true
```

This shell gives you the Rust toolchain, clang, libclang and the PHP dev headers. With them, you can build each
extension from source.

`with-arrow-ext` and `with-flow-php-ext` both default to `!with-rust`. `--arg with-rust true` therefore turns off the
prebuilt extensions, and you do not have to give these flags. Give `--arg with-arrow-ext false` or
`--arg with-flow-php-ext false` only to override an explicit `true`. If you combine `--arg with-rust true` with one of
them set to `true`, the `shell.nix` assertion fails, because a prebuilt extension and a source build collide.

> [!IMPORTANT]
> After `make build`, PHP does not use the new extension. The `test` target of each Makefile loads the binary
> explicitly with `php -d extension=...`. To use a new build in other places, run `make install`, give
> `-d extension=` yourself, or start `nix-shell` again. For more information, read
> [Rebuilding after a source change](#rebuilding-after-a-source-change).

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

Replace `<extension>` with `arrow-ext` or `flow-php-ext`.

Build:

```bash
nix-shell --arg with-rust true --run "cd src/extension/<extension> && make build"
```

Run the PHPT tests. `test` depends on `build`, so this command builds first:

```bash
nix-shell --arg with-rust true --run "cd src/extension/<extension> && make test"
```

Remove the build artifacts:

```bash
nix-shell --arg with-rust true --run "cd src/extension/<extension> && make clean"
```

Run the PHP test suites against the prebuilt extension. These commands use the default shell, not the Rust shell:

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

The two PHPT runners use the same shell loop:

- Each test runs with `php -d extension=<the built .so>`, not with `php -n`. The `php.ini` of the shell and its
  extensions also load.
- The runner obeys `--SKIPIF--` blocks and reports the number of skipped tests.
- The runner compares the output exactly. It accepts `--EXPECTF--`, but it does not expand `%s`.

`flow-php-ext` also takes `ARROW_SO=<path>`, which loads arrow-ext first. The Parquet ETL phpts then run and do not
skip: `make test ARROW_SO=../arrow-ext/ext/modules/arrow.so`.

## Rebuilding after a source change

`.nix/pkgs/php-arrow-ext` and `.nix/pkgs/php-flow-php-ext` build their extension from the local repository source.
A shell that you started before a change to a `.rs` file still contains the previous build. If you run `just test` in
that shell, the failures come from the old binary, not from your change.

Start `nix-shell` again to build the derivation again, or build and test the extension directly:

```bash
nix-shell --arg with-rust true --run "cd src/extension/flow-php-ext && make build && make test"
```

This command also covers all crates under `crates/`. The flow-php-ext build compiles them from the same workspace.

## Extension ABI

Each extension registers an `int` constant with the version of the contract that its PHP package expects. The PHP
package holds the same number:

| Extension | Constant (Rust `lib.rs`) | PHP side                                    |
|-----------|--------------------------|---------------------------------------------|
| flow_php  | `FLOW_PHP_ABI`           | `Flow\ETL\Foundation\FlowPhpExtension::ABI` |
| arrow     | `FLOW_ARROW_ABI`         | `Flow\Parquet\Engine\ArrowExtension::ABI`   |

Increase both numbers in one change each time the contract changes. The contract is a class or interface that the
extension registers, or the schema, options or values that go across it. Each `Adaptive*` pick calls `available()`.
If the loaded extension has a different ABI, or no ABI, the pick refuses it with a `RuntimeException`. The message
names the extension and its version. phpt `099` (flow_php) and `054` (arrow) make sure that the two numbers are equal.

An extension also registers PHP interfaces. The PHP declarations of these interfaces are guarded by
`interface_exists(X::class, false)`. If an extension does not register one of them, PHP still gets the PHP
declaration. The extension then fails at the ABI check, not with "Class not found".

## The flow-php-ext workspace

`src/extension/flow-php-ext` is a cargo workspace:

- The root package is the extension. It contains only the PHP glue: ext-php-rs classes, Zval conversions and
  exception text.
- The members under `crates/` are pure Rust with no PHP types.

Each `#[test]` is in a member, so the test binaries link without a PHP engine. If a member uses a Zend symbol, it
fails to link on Linux. CI runs this link for each push. Each member returns its refusals as data (`Error`,
`Refusal`, `Malformed`). `flow_php` converts each refusal into the PHP message. `flow-batch-frame` holds the batch
layout of Flow. It has the buffers of one column in the canonical form of `Column::encode()` and back. It also has the
builders that append rows to the buffers, and the BATCH frame body around them.

```bash
nix-shell --arg with-rust true --run "cd src/extension/flow-php-ext && cargo test --workspace --features arrow-validate"
nix-shell --arg with-rust true --run "cd src/extension/flow-php-ext && cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings"
```

`arrow-validate` turns on `arrow-data/force_validate`. This feature makes sure again that each array that the
builders give with `build_unchecked` is valid. `make test` builds the extension with it, and `make build` builds it
without it. The same feature and the same three commands (without `--workspace`) apply to `src/extension/arrow-ext`.

arrow-ext does not depend on the workspace. Its `canonical_type()` (`src/parquet/canonical.rs`) must produce the same
Arrow types as `kind::data_type()` of flow-batch-frame. flow-php-ext phpt 091 tests this.

## Releasing

You do not have to change a version number. After the tag `X.Y.Z`, builds without a tag report
`X.(Y+1).0-dev+<commits>.g<sha>`. The nix packages report `X.(Y+1).0-dev`. `shell.nix` reads this value from the newest
`X.Y.Z` tag in `.git`. In a checkout without tags, the nix-shell fails and tells you to run `git fetch --tags`.
