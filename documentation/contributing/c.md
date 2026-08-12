# C - pg-query Extension Development

[TOC]

This document describes how to develop the `pg-query-ext` PHP extension, which is written in C
and wraps [libpg_query](https://github.com/pganalyze/libpg_query) - a C library that uses the
actual PostgreSQL parser.

## Overview

The `pg-query-ext` extension exposes PostgreSQL's SQL parser to PHP, providing functions for
parsing, normalizing, fingerprinting, and splitting SQL queries. It statically links against
`libpg_query` which embeds the PostgreSQL 18 grammar.

For usage documentation, see [pg-query Extension](/documentation/components/extensions/pg-query-ext.md).

## Development Setup

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true
```

This provides the C toolchain (gcc, autoconf, automake, libtool, protobuf) and PHP dev headers
for building the extension from source.

## Project Structure

```
src/extension/pg-query-ext/
├── Makefile                # Build orchestration (autotools + libpg_query fetch)
├── ext/                    # C source code
│   ├── config.m4           # PHP extension build config (autoconf)
│   ├── pg_query.c          # Extension implementation
│   └── php_pg_query.h      # Header file
├── php/                    # PHP stubs for static analysis
├── tests/
│   └── phpt/               # PHPT test files
└── vendor/
    └── libpg_query/        # Auto-downloaded libpg_query source (gitignored)
```

## Commands

Build the extension (first build downloads libpg_query automatically):

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true --run "cd src/extension/pg-query-ext && make build"
```

A system `libpg_query` is only used when its `PG_MAJORVERSION` matches the pinned `LIBPG_QUERY_VERSION`
in `config.m4`. A mismatched `--with-pg-query=DIR` is a hard configure error; a mismatched
auto-discovered library (e.g. a Homebrew keg for a different PostgreSQL major) is skipped in favor of
downloading the pinned version. This prevents silently baking the wrong PostgreSQL grammar into
`pg_query.so` ([#2483](https://github.com/flow-php/flow/issues/2483)).

Run PHPT tests:

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true --run "cd src/extension/pg-query-ext && make test"
```

Rebuild extension only (skip libpg_query, faster after C source changes):

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true --run "cd src/extension/pg-query-ext && make rebuild"
```

Clean extension build artifacts (keep libpg_query):

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true --run "cd src/extension/pg-query-ext && make clean"
```

Remove everything including downloaded libpg_query:

```bash
nix-shell --arg with-pg-query-ext false --arg with-c true --run "cd src/extension/pg-query-ext && make distclean"
```

## Make Targets

| Target      | Description                                           |
|-------------|-------------------------------------------------------|
| `build`     | Full build (download libpg_query if needed + compile) |
| `test`      | Run PHPT tests                                        |
| `install`   | Install to system PHP                                 |
| `rebuild`   | Rebuild extension only (skip libpg_query)             |
| `clean`     | Remove extension build artifacts                      |
| `distclean` | Remove everything including libpg_query               |

## Upgrading libpg_query (keep the C, nix, and PHP sides in sync)

The extension is only half of the story. A given `libpg_query` version pins a specific PostgreSQL
**grammar**, and that grammar is shared across three places that must always agree:

| Place                                                                                                                           | What it is                                                                | Used by                           |
|---------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|-----------------------------------|
| `src/extension/pg-query-ext/vendor/libpg_query` (Makefile `PG_VERSION`)                                                         | the C library compiled into the extension                                 | CI (`pie` build)                  |
| `.nix/pkgs/php-pg-query-ext/package.nix` (`libpg_query` `version`/`rev`/`hash`)                                                 | the C library compiled into the **nix dev shell**                         | local `nix-shell`                 |
| `src/lib/postgresql/resources/proto/pg_query.proto` + the generated stubs in `src/lib/postgresql/src/Flow/PostgreSql/Protobuf/` | the protobuf schema the PHP side serializes/deserializes parse trees with | the `flow-php/postgresql` library |

The PHP library round-trips parse trees as protobuf (`pg_query_parse_protobuf` → mutate → `pg_query_deparse`).
Protobuf encodes each `Node` oneof case by its **field number**. When a new PG major inserts a node, every
later field number shifts - so if the C library is bumped but the PHP stubs are left stale, PHP serializes
parse trees with the wrong tags and `pg_query_deparse` can fail to unpack them and **segfault** (a real
incident: PG17→PG18 left the stubs stale and crashed CI intermittently, while a stale nix pin hid it locally).

When you change the `libpg_query` version, do **all** of the following, then run the full PHP suite:

1. Bump the C side (`src/extension/pg-query-ext/Makefile` `PG_VERSION` / vendored copy).
2. Bump the nix pin in `.nix/pkgs/php-pg-query-ext/package.nix` to the **same** `libpg_query` tag, and
   update its `hash` (`nix-prefetch-url --unpack <github-archive-url>` → `nix hash convert --to sri`).
3. Regenerate the PHP stubs from the new grammar - sync `resources/proto/pg_query.proto` from
   `vendor/libpg_query/protobuf/pg_query.proto` (keep the `option php_namespace` / `php_metadata_namespace`
   lines) then run `just gen-protobuf-pg` inside `nix-shell --arg with-protoc true`.
4. Handle any grammar changes in the query builder / AST (e.g. new `Constraint` fields, restructured
   clauses) - `mago analyze` and the suite will surface these.

### Always validate against the full PHP suite, not just the PHPT tests

`make test` only runs the C-level PHPT tests; it does **not** exercise the PHP `flow-php/postgresql`
parse/deparse round-trips where grammar drift bites. After any extension change, run the full suite in a
shell whose extension matches what you changed:

```bash
# uses the nix-built extension (the package.nix pin) - must match the vendored/CI version
nix-shell --run "just test"

# or, at minimum, the suites that exercise parse/deparse round-trips:
nix-shell --run "tools/phpunit/vendor/bin/phpunit --testsuite lib-postgresql-unit,lib-postgresql-integration,bridge-phpunit-postgresql-unit,bridge-phpunit-postgresql-integration"
```

A segfault here shows up as `signal 11` / exit `139`. To capture a backtrace, run the failing command
under `lldb` (macOS) or `gdb` (Linux):

```bash
nix-shell --run 'lldb -b -o "settings set target.process.stop-on-exec false" -o run -o bt -o quit \
  -- "$(command -v php)" tools/phpunit/vendor/bin/phpunit --testsuite lib-postgresql-unit'
```
