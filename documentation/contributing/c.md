# C - pg-query Extension Development

[TOC]

This document describes how to develop the `pg-query-ext` PHP extension, which is written in C
and wraps [libpg_query](https://github.com/pganalyze/libpg_query) — a C library that uses the
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

| Target      | Description                                          |
|-------------|------------------------------------------------------|
| `build`     | Full build (download libpg_query if needed + compile)|
| `test`      | Run PHPT tests                                       |
| `install`   | Install to system PHP                                |
| `rebuild`   | Rebuild extension only (skip libpg_query)            |
| `clean`     | Remove extension build artifacts                     |
| `distclean` | Remove everything including libpg_query              |
