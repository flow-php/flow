# C - pg-query Extension Development

[TOC]

This page tells you how to develop the `pg-query-ext` PHP extension. The extension is written in C. It wraps
[libpg_query](https://github.com/pganalyze/libpg_query), a C library that uses the real PostgreSQL parser.

## Overview

The `pg-query-ext` extension gives PHP access to the SQL parser of PostgreSQL. Its functions parse, normalize,
fingerprint and split SQL queries. It links statically against `libpg_query`, which contains the PostgreSQL 18
grammar.

For the usage documentation, read [pg-query Extension](/documentation/components/extensions/pg-query-ext.md).

## Development Setup

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true
```

This shell gives you the C toolchain (gcc, autoconf, automake, libtool, protobuf). It also gives you the PHP dev
headers for a build of the extension from source.

## Project Structure

```
src/extension/pg-query-ext/
├── Makefile                # Build orchestration (autotools + libpg_query fetch)
├── ext/                    # C source code
│   ├── config.m4           # PHP extension build config (autoconf)
│   ├── pg_query.c          # Extension implementation
│   ├── pg_query.stub.php   # PHP stubs for static analysis
│   └── php_pg_query.h      # Header file
├── tests/
│   └── phpt/               # PHPT test files
└── vendor/
    └── libpg_query/        # Auto-downloaded libpg_query source (gitignored)
```

## Commands

Build the extension. The first build downloads libpg_query:

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true --run "cd src/extension/pg-query-ext && make build"
```

A system `libpg_query` must have a `PG_MAJORVERSION` that matches the pinned `LIBPG_QUERY_VERSION` in `config.m4`.
If it does not match, the build does not use it:

- If `--with-pg-query=DIR` points to a library with a different version, configure stops with an error.
- If the build finds a library with a different version, for example a Homebrew keg for a different PostgreSQL major,
  it skips that library and downloads the pinned version.

This rule prevents a silent build of `pg_query.so` with the wrong PostgreSQL grammar
([#2483](https://github.com/flow-php/flow/issues/2483)).

Run the PHPT tests:

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true --run "cd src/extension/pg-query-ext && make test"
```

After a change to the C source, rebuild only the extension. This skips libpg_query and is faster:

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true --run "cd src/extension/pg-query-ext && make rebuild"
```

Remove the build artifacts of the extension and keep libpg_query:

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true --run "cd src/extension/pg-query-ext && make clean"
```

Remove everything, also the downloaded libpg_query:

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true --run "cd src/extension/pg-query-ext && make distclean"
```

## Make Targets

| Target      | Description                                                 |
|-------------|-------------------------------------------------------------|
| `build`     | Full build (download a missing libpg_query, compile)        |
| `test`      | Run PHPT tests                                              |
| `install`   | Install to system PHP                                       |
| `rebuild`   | Rebuild extension only (skip libpg_query)                   |
| `clean`     | Remove extension build artifacts                            |
| `distclean` | Remove everything, also libpg_query                         |

## Upgrading libpg_query (keep the C, nix, and PHP sides in sync)

The extension is only one part. Each `libpg_query` version pins one PostgreSQL grammar. Three places use that
grammar, and they must always agree:

| Place                                                                                                                           | What it is                                                                  | Used by                           |
|---------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|-----------------------------------|
| `src/extension/pg-query-ext/vendor/libpg_query` (`LIBPG_QUERY_VERSION`)                                                         | the C library compiled into the extension                                   | CI (`pie` build)                  |
| `.nix/pkgs/php-pg-query-ext/package.nix` (`libpg_query` `version`/`rev`/`hash`)                                                 | the C library compiled into the nix dev shell                               | local `nix-shell`                 |
| `src/lib/postgresql/resources/proto/pg_query.proto` + the generated stubs in `src/lib/postgresql/src/Flow/PostgreSql/Protobuf/` | the protobuf schema that the PHP side uses to encode and decode parse trees | the `flow-php/postgresql` library |

The PHP library sends parse trees through protobuf in both directions (`pg_query_parse_protobuf` → mutate →
`pg_query_deparse`). Protobuf encodes each `Node` oneof case by its field number. When a new PostgreSQL major adds a
node, all later field numbers change. If you update the C library and not the PHP stubs, PHP encodes parse trees with
the wrong tags. `pg_query_deparse` can then fail to decode them, and the process stops with a segfault. This
happened in a real incident: the upgrade from PG17 to PG18 did not update the stubs, and CI crashed at random. A
stale nix pin hid the problem on local machines.

When you change the `libpg_query` version, do all of these steps. Then run the full PHP suite:

1. Update the C side: set `LIBPG_QUERY_VERSION` in `src/extension/pg-query-ext/Makefile` and `ext/config.m4`. Use a
   release tag, never a branch. The `18-latest` branch once moved while CI ran. Then run `make distclean build`.
2. Update the nix pin in `.nix/pkgs/php-pg-query-ext/package.nix` to the same `libpg_query` tag, and update its
   `hash` (`nix-prefetch-url --unpack <github-archive-url>` → `nix hash convert --to sri`).
3. Generate the PHP stubs again from the new grammar. Copy `vendor/libpg_query/protobuf/pg_query.proto` to
   `resources/proto/pg_query.proto`, and keep the `option php_namespace` / `php_metadata_namespace` lines. Then run
   `just gen-protobuf-pg` inside `nix-shell --arg with-protoc true`.
4. Change the query builder and the AST for each grammar change, for example new `Constraint` fields or clauses with
   a new structure. `mago analyze` and the test suite show these changes.

### Always validate against the full PHP suite, not just the PHPT tests

`make test` runs only the PHPT tests at the C level. It does not run the parse and deparse round trips of the PHP
`flow-php/postgresql` library, and grammar drift breaks these round trips. After each extension change, run the full
suite in a shell that has the extension that you changed:

```bash
# uses the nix-built extension (the package.nix pin) - must match the vendored/CI version
nix-shell --run "just test"

# or, at minimum, the suites that exercise parse/deparse round-trips:
nix-shell --run "tools/phpunit/vendor/bin/phpunit --testsuite lib-postgresql-unit,lib-postgresql-integration,bridge-phpunit-postgresql-unit,bridge-phpunit-postgresql-integration"
```

A segfault shows as `signal 11` / exit `139`. To get a backtrace, run the failed command with `lldb` (macOS) or `gdb`
(Linux):

```bash
nix-shell --run 'lldb -b -o "settings set target.process.stop-on-exec false" -o run -o bt -o quit \
  -- "$(command -v php)" tools/phpunit/vendor/bin/phpunit --testsuite lib-postgresql-unit'
```
