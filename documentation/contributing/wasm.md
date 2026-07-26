# WASM - Interactive Playground Build

[TOC]

This document describes how to build PHP to WebAssembly (WASM) for use in the Flow PHP Interactive Playground.

## Overview

The WASM build creates a browser-compatible PHP runtime that powers the interactive playground
at [flow-php.com](https://flow-php.com). It compiles PHP 8.5.8 with the necessary extensions to run Flow PHP ETL
pipelines directly in the browser. The version is kept in step with the Docker image
(`Dockerfile` `FLOW_PHP_VERSION`) so both distributions ship the same PHP.

## Development Setup

```bash
nix-shell --arg with-wasm true
```

This provides all necessary dependencies including Emscripten, autoconf, cmake, wget, and libxml2.

## Commands

Build the WASM binary:

```bash
nix-shell --arg with-wasm true --run "just wasm"
```

Use `just wasm` rather than calling `wasm/build.sh` directly. The recipe also rebuilds `flow.phar`
into `web/landing/assets/wasm/tools/`, and the playground loads that phar — building the runtime on
its own leaves a new PHP paired with a stale library.

The build script will:

1. Download and compile libxml2 for WebAssembly
2. Download and compile libpg_query for WebAssembly
3. Download PHP source
4. Apply the php-src patches in `wasm/patches/`
5. Copy the pg_query and snappy extensions
6. Configure and compile PHP with required extensions
7. Link everything into `php.wasm` and `php.js`
8. Copy outputs to `web/landing/assets/wasm/`

Each dependency is skipped when its build output is already present, so re-runs are cheap. Bumping
a version changes the directory name (or, for libpg_query and libzip, invalidates the archive
check), which is what triggers the rebuild.

### libpg_query version

`build.sh` does not pin its own libpg_query version. It reads `LIBPG_QUERY_VERSION` from
`src/extension/pg-query-ext/Makefile`, so the playground and the native extension can never
disagree about which PostgreSQL grammar parses, and fails fast if that variable cannot be read.

The pin is a **branch** (`18-latest`), not a tag. Changing it re-clones automatically; upstream
moving the branch under a fixed version does not, and needs `rm -rf wasm/libpg_query-*`.

## Included PHP Extensions

| Extension | Purpose                                           |
|-----------|---------------------------------------------------|
| bcmath    | Required by flow-php/parquet for decimal handling |
| mbstring  | String handling                                   |
| phar      | PHAR archive support                              |
| filter    | Data filtering                                    |
| tokenizer | PHP tokenization                                  |
| zlib      | Compression support                               |
| iconv     | Character encoding conversion                     |
| libxml    | Base for XML extensions                           |
| xml       | XML parsing                                       |
| dom       | DOM manipulation                                  |
| xmlreader | XML streaming reader                              |
| xmlwriter | XML streaming writer                              |
| zip       | Required by flow-php/etl-adapter-excel (XLSX files are ZIP archives) |
| pg_query  | PostgreSQL query parsing (Flow PHP extension)     |
| snappy    | Snappy compression for Parquet                    |

## Output Files

| File           | Description                 |
|----------------|-----------------------------|
| `out/php.wasm` | Compiled WebAssembly binary |
| `out/php.js`   | JavaScript loader/glue code |

These are copied to `web/landing/assets/wasm/` for use by the playground.

## Architecture Notes

### Opcache is always compiled in, and always inert

PHP 8.5 made Opcache non-optional
([RFC](https://wiki.php.net/rfc/make_opcache_required), php-src `7b4c14dc1016`). The
`PHP_ARG_ENABLE([opcache], ...)` that `--disable-all` used to switch off is gone; `config.m4` now
calls `PHP_NEW_EXTENSION([opcache], ...)` unconditionally. On 8.4 `--disable-all` excluded Opcache
silently, which is why none of this was written down before.

Two configure flags exist because of that, and **neither is redundant next to `--disable-all`** —
both pass `[no]` as their 5th `PHP_ARG_ENABLE` argument, so `--disable-all` cannot reach either:

| Flag | Why |
|------|-----|
| `--disable-opcache-jit` | Opcache's JIT is gated on `$host_cpu`. `emconfigure` does not pass `--host`, so `config.guess` reports the **build** machine, and `config.m4` accepts every common one (`x86*`, `aarch64`, `amd64`) as JIT-capable. Without this flag `emcc` is handed `jit/ir/*.c` plus dynasm for the host ISA to compile into a wasm32 binary. Same class of workaround as `--disable-fiber-asm`. |
| `--disable-huge-code-pages` | Copying code pages into huge pages is meaningless under wasm. |

**Do not delete `--disable-opcache-jit` because it looks redundant.** It is not.

At runtime Opcache is present but does nothing. None of its three shared-memory backends is
available under Emscripten — each probe compiles a program that calls `fork()`, which Emscripten
does not implement — so configure reports:

```
checking for sysvipc shared memory support... no
checking for mmap() using MAP_ANON shared memory support... no
checking for mmap() using shm_open() shared memory support... no
configure: WARNING: No supported shared memory caching support was found when configuring opcache.
Opcache will be disabled.
```

Since php-src `e4078a6a70d5` a missing backend is a warning rather than a build failure, and at
runtime `NO_SHM_BACKEND` disables Opcache while PHP still starts. The backend sources
(`shared_alloc_mmap.c`, `shared_alloc_posix.c`, `shared_alloc_shm.c`) are each wholly inside
`#ifdef USE_MMAP` / `USE_SHM_OPEN` / `USE_SHM`, so with none defined they compile to nothing.

This is deliberate. Forcing Opcache on (as WordPress Playground does, by rewriting
`php_cv_shm_mmap_anon` before `buildconf`) was considered and rejected: the playground evaluates a
fresh snippet per run, so a real opcode cache buys nothing for the size and risk it adds.

### One PHP lifecycle per Run

`pib_eval()` calls `php_embed_init()` and `php_embed_shutdown()` around **every** evaluation
(`wasm/pib_eval.c`), so a single browser tab starts and stops the PHP engine once per Run. No other
SAPI does this — CLI and FPM start once per process, so module shutdown is the last thing that ever
runs there.

That makes the playground the only place where a non-re-entrant `MINIT`/`MSHUTDOWN` pair is fatal,
and it fails in a way that points nowhere near the cause: Run 1 succeeds, Run 2 dies with
`null function` or `memory access out of bounds` — a wasm indirect call through a freed function
pointer, with no PHP-level stack trace.

This already happened once. `pg_query`'s `MSHUTDOWN` called `pg_query_exit()`, which frees
libpg_query's `TopMemoryContext` while `pg_query_init()` guards on a flag it never resets — so the
second startup ran against freed memory. The fix was to not free at all (see the comment in
`src/extension/pg-query-ext/ext/pg_query.c`). Note that resetting the flag is *not* sufficient:
`MemoryContextInit()` then re-runs over freed contexts, which survives a trivial snippet and fails
under a real pipeline.

**When adding an extension to this build, verify it survives repeated init/shutdown, and test with a
realistic workload** — load the phar and run a pipeline. A `<?php echo 1;` probe will not catch it.

### php-src patches

`wasm/patches/*.patch` are applied to the extracted PHP source after download and before
`buildconf`. Application is idempotent — a cleanly applying *reverse* patch means it is already
there — because `build.sh` reuses an already extracted source tree across runs.

Currently one patch: `php-8.5-opcache-unistd.patch`. `ext/opcache/zend_accelerator_debug.c` calls
`getpid()` in its non-ZTS branch but includes `<process.h>` only under `ZEND_WIN32`. clang has
treated implicit function declarations as an error since 16, so the build fails without it. On 8.4
this never surfaced, because the file was never compiled. Re-check on the next PHP bump and delete
the file if it has landed upstream.

### 32-bit Environment

WebAssembly runs as a 32-bit environment where `PHP_INT_MAX = 2147483647`. This affects:

- Large integer values from Thrift/Parquet metadata may overflow to floats
- The parquet library includes `(int)` casts in `fromThrift()` methods to handle this
- Hex constants like `0x80000000` and `0xFFFFFFFF` require explicit `(int)` casts
- `pack()`/`unpack()` 64-bit format codes (`q`, `Q`, `J`, `P`) are unavailable and raise
  `ValueError: 64-bit format codes are not available for 32-bit versions of PHP`. This currently
  breaks `sortBy()` and anything else reaching `Flow\Floe\Encoding\Int64Encoder`, which is a known
  outstanding limitation of the playground rather than a build problem.

### Wrapper Code

The `pib_eval.c` file provides the C wrapper for evaluating PHP code:

- `pib_eval(char *code)` - Evaluates PHP code and returns the result
- `pib_force_exit()` - Forces exit from the WASM runtime

## Troubleshooting

### Build fails with configure errors

Check `config.log` in the PHP directory for detailed error messages.

### Missing extensions

Ensure all required libraries (libxml2, libpg_query) are built before PHP configuration.

### Memory issues in browser

The playground may fail on very large datasets due to WASM memory limits.
