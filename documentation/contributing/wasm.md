# WASM - Interactive Playground Build

[TOC]

This document describes how to build PHP to WebAssembly (WASM) for use in the Flow PHP Interactive Playground.

## Overview

The WASM build creates a browser-compatible PHP runtime that powers the interactive playground
at [flow-php.com](https://flow-php.com). It compiles PHP 8.5.8 with the necessary extensions to run Flow PHP ETL
pipelines directly in the browser. The version is kept in step with the Docker image
(`Dockerfile` `FLOW_PHP_VERSION`) so both distributions ship the same PHP.

The build targets **64-bit** wasm (`-sMEMORY64=2`), so the playground has the same `PHP_INT_MAX` as
every other distribution. See [64-bit Environment](#64-bit-environment) and
[Why MEMORY64=2 and not MEMORY64=1](#why-memory642-and-not-memory641).

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

### Opcache is always compiled in, and is deliberately switched on

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

#### The shared-memory backend has to be forced on

Left alone, Opcache builds but can never run. Each of its three shared-memory probes compiles a
program that calls `fork()`, which Emscripten does not implement, so all three fail and configure
defines `NO_SHM_BACKEND`:

```
checking for sysvipc shared memory support... no
checking for mmap() using MAP_ANON shared memory support... no
checking for mmap() using shm_open() shared memory support... no
configure: WARNING: No supported shared memory caching support was found when configuring opcache.
Opcache will be disabled.
```

Since php-src `e4078a6a70d5` that is a warning rather than a build failure, and at runtime
`NO_SHM_BACKEND` disables Opcache while PHP still starts. The backend sources
(`shared_alloc_mmap.c`, `shared_alloc_posix.c`, `shared_alloc_shm.c`) are each wholly inside
`#ifdef USE_MMAP` / `USE_SHM_OPEN` / `USE_SHM`, so with none defined they compile to nothing.

`build.sh` therefore pre-sets the autoconf cache variable before `./configure`:

```bash
export php_cv_shm_mmap_anon=yes
```

which turns the probe into `checking for mmap() using MAP_ANON shared memory support... (cached) yes`
and defines `HAVE_SHM_MMAP_ANON` → `USE_MMAP`. Only that one is forced; the other two stay `no`. The
three checks are siblings rather than nested (`ext/opcache/config.m4:176-234`), and `USE_MMAP` alone
is enough.

**This is honest, not a lie to the build system.** The probe fails only on its `fork()` half. What
Opcache actually needs at runtime, it gets:

- `mmap(MAP_SHARED|MAP_ANONYMOUS)` — Emscripten's `__syscall_mmap2` takes an anonymous branch that
  **ignores `MAP_SHARED`** and serves the mapping out of linear memory
  (`system/lib/libc/emscripten_mmap.c`). A single-process runtime needs no actual sharing.
- `fcntl(F_SETLK/F_SETLKW)` — Emscripten returns success by design, commenting that these are
  process-level locks and a wasm program is one process (`src/lib/libsyscall.js`).
- the lock file — no `memfd_create` or `O_TMPFILE`, so it falls through to `mkstemp()` under
  `opcache.lockfile_path`, and MEMFS provides `/tmp` by default.

All three were verified with a standalone `emcc` program *before* the PHP rebuild rather than
discovered after it — worth repeating on any toolchain bump.

#### Why this reverses the earlier decision

An earlier revision of this file recorded the opposite decision: Opcache was left inert because "the
playground evaluates a fresh snippet per run, so a real opcode cache buys nothing." The first half of
that is true and the second does not follow. The snippet is fresh; the **3.7 MB `flow.phar` behind it
is not**, and re-compiling that class graph on every Run was the entire cost. Measured, with one PHP
request per Run:

| | leak per Run | Runs before the page died |
|---|---|---|
| no Opcache | 2.36 MB | 813 |
| Opcache with a working SHM backend | **0.00 MB** | no death in 5,000 |

It is also **28% faster**, not slower: a CSV → JSON → read-back workload went from a 319 ms to a
228 ms median once the phar stopped being recompiled on every Run. The artifact grew 0.03%.

Opcache only pays off *on top of* the one-module-per-page lifecycle (see below), because its shared
memory is created at `MINIT` — under the old one-module-per-Run model it was rebuilt every Run,
which is very likely why it looked worthless.

#### Settings, and why they are set in C

`opcache.memory_consumption` is read once at `MINIT`, so it has to be in place before
`php_embed_init()`. `php_embed_init()` overwrites `php_embed_module.ini_entries` with its own
`HARDCODED_INI`, so the only usable hook is `php_embed_module.ini_defaults`, which is what
`pib_eval.c` sets. There is no `php.ini` in this build.

`opcache.validate_timestamps=0`: the phar is written once by the loader and never changes for the
life of the page, and the one file that does change (`code.php`, rewritten by the Format action) is
never `require`d — snippets are compiled through `zend_compile_string`, which Opcache does not cache
either way. So the per-request `stat()` of every cached file buys nothing.

The SHM is `mmap`ed out of wasm linear memory at `MINIT` and never returned, so
`opcache.memory_consumption` is a permanent per-page cost paid by every visitor, including ones who
never press Run. Size it from measurement, not from the upstream default.

`opcache.enable_cli` is irrelevant here: `accel_sapi_is_cli()` matches only `cli` and `phpdbg`, and
this SAPI is named `embed`.

### One module lifecycle per page, one request lifecycle per Run

`pib_eval()` (`wasm/pib_eval.c`) calls `php_embed_init()` **once**, on the first Run of a page, and
then cycles only the request around every evaluation:

```
first Run only :  php_embed_init()          =  sapi_startup -> MINIT -> php_request_startup
every Run      :  php_request_shutdown() ; php_request_startup()
never          :  php_embed_shutdown()
```

The embed SAPI splits cleanly along exactly those lines (`sapi/embed/php_embed.c`). This is what
every web SAPI does; the old model — a full `php_embed_init()`/`php_embed_shutdown()` per Run — is
the exotic one, and it cost the page ~36 MB of unreclaimable wasm memory per Run.

**Per-Run isolation is preserved**, because user-defined classes, functions, constants and globals
are all request-scoped and request shutdown still frees them. Verified case by case against the old
artifact rather than assumed — class and function redeclaration, `$GLOBALS`, `static` variables,
`ini_set`, `declare(strict_types=1)`, autoloader registrations, unclosed file handles, `exit()`,
`register_shutdown_function`, an uncaught throwable, and output bleed all behave identically to
before.

Two ordering details are load-bearing:

- The request is cycled at the **end** of `pib_eval()`, not lazily at the start of the next one, so
  shutdown functions and destructors are attributed to the Run that caused them.
- `php_request_shutdown()` runs **before** the `fflush()` pair, because it is what emits
  `register_shutdown_function` output.

Both `php_request_startup()` and `php_request_shutdown()` wrap their bodies in `zend_try`
internally, so neither can bail out past `pib_eval()`'s own `zend_first_try`.

`php_embed_shutdown()` is now never called: a browser page has no teardown hook, and the controller's
recycling path (below) discards the whole wasm instance, which reclaims everything. That is
deliberate — calling it would run `MSHUTDOWN`, and see the next paragraph for why that is a hazard
worth not courting.

#### `MSHUTDOWN` now runs at most once — do not take that as licence

Under the old model a non-re-entrant `MINIT`/`MSHUTDOWN` pair was fatal, and it failed in a way that
pointed nowhere near the cause: Run 1 succeeded, Run 2 died with `null function` or
`memory access out of bounds` — a wasm indirect call through a freed function pointer, with no
PHP-level stack trace.

This already happened once. `pg_query`'s `MSHUTDOWN` called `pg_query_exit()`, which frees
libpg_query's `TopMemoryContext` while `pg_query_init()` guards on a flag it never resets — so the
second startup ran against freed memory. The fix was to not free at all (see the comment in
`src/extension/pg-query-ext/ext/pg_query.c`). Note that resetting the flag is *not* sufficient:
`MemoryContextInit()` then re-runs over freed contexts, which survives a trivial snippet and fails
under a real pipeline.

The lifecycle change makes that class of bug much harder to hit. **It is not a reason to restore
`pg_query_exit()`** — that is a separate decision with its own history, and the recycling path still
creates fresh modules within one page.

**When adding an extension to this build, still test with a realistic workload** — load the phar and
run a pipeline across several Runs. A `<?php echo 1;` probe will not catch a request-scoped leak.

### php-src patches

`wasm/patches/*.patch` are applied to the extracted PHP source after download and before
`buildconf`. Application is idempotent — a cleanly applying *reverse* patch means it is already
there — because `build.sh` reuses an already extracted source tree across runs.

`build.sh` globs the directory, so adding a file is enough — there is no list to keep in step.

| Patch | Why |
|---|---|
| `php-8.5-opcache-unistd.patch` | `ext/opcache/zend_accelerator_debug.c` calls `getpid()` in its non-ZTS branch but includes `<process.h>` only under `ZEND_WIN32`. clang has treated implicit function declarations as an error since 16, so the build fails without it. On 8.4 this never surfaced, because the file was never compiled. |
| `php-8.5-emscripten-mm-chunk-alignment.patch` | Zend's chunk allocator aligns by over-allocating and then partially `munmap`ing, which Emscripten cannot do — so ~2 MB was orphaned per chunk, permanently. See [the memory leak section](#the-wasm-memory-leak-and-why-the-page-no-longer-has-a-run-budget). |

Re-check both on the next PHP bump and delete either if it has landed upstream.

### 64-bit Environment

**The playground is 64-bit.** `PHP_INT_MAX === 9223372036854775807` and `PHP_INT_SIZE === 8`,
verified in the browser rather than inferred from build flags. This section used to document the
opposite; the history matters, because the workarounds it justified are still in the codebase.

What is now true:

- `pack()`/`unpack()` 64-bit format codes (`q`, `Q`, `J`, `P`) work. `Flow\Floe` therefore works, and
  so does **`sortBy()`** and everything else that spills runs to disk through
  `Flow\Floe\Encoding\Int64Encoder`. Before this, every Floe write in the browser died with
  `ValueError: 64-bit format codes are not available for 32-bit versions of PHP`.
- Large integers from Thrift/Parquet metadata no longer overflow to float. On the 32-bit build,
  reading a parquet file emitted hundreds of
  `Warning: The float 2147483648 is not representable as an int` from
  `Flow\Parquet\Thrift\CompactProtocol` and `Flow\ETL\Bucketing\HashBucketing`; the 64-bit build
  emits none.
- Hex constants like `0x80000000` are plain integers and need no `(int)` cast.
- `DateTimeEncoder` packs `getTimestamp()`, which now represents dates past 2038-01-19.

The `(int)` casts in the parquet library's `fromThrift()` methods are **left in place**. They are no
longer load-bearing for the playground, but the library still supports 32-bit PHP builds generally,
and removing them is a separate decision.

### Why `MEMORY64=2` and not `MEMORY64=1`

`build.sh` builds with `-sMEMORY64=2` (`WASM64_MODE`). Emscripten describes the three values as
wasm32 (`0`), full end-to-end wasm64 (`1`), and wasm64 for clang/lld **lowered to wasm32 by Binaryen**
(`2`). Only `2` gives PHP an 8-byte `zend_long` without demanding Memory64 support from the engine
running it:

| | `MEMORY64=1` | `MEMORY64=2` |
|---|---|---|
| `sizeof(long)` | 8 | 8 |
| Engine requirement | Memory64 | **runs on today's wasm32 engines** |
| Node needed to run build-time probes | ≥ 23 | **works on the nix shell's node 22** |

The node requirement is not a detail. PHP's `configure` reports `checking whether we are cross
compiling... no` under `emconfigure` and executes its `AC_RUN_IFELSE` probes *through node*, so with
`=1` PHP's own configure would fail or misdetect before anything else was reached. And since
flow-php.com is public, `=1` would additionally need Memory64 verified in Safari and Firefox.

`=2` is sufficient because the playground does not need a >4 GB address space —
`MAXIMUM_MEMORY=2147483648` says so. **The memory settings are deliberately unchanged from the wasm32
build**: `=2` lowers to a 32-bit memory, so the 2 GB ceiling is a hard limit of the mode rather than a
leftover. Raising it would require `=1`.

Switching to `=1` is a deliberate decision, not a tuning step.

### The target flag has to reach the dependencies, not just PHP

`WASM64_MODE` is exported as **`EMCC_CFLAGS`**, near the top of `build.sh`, and this placement is
load-bearing:

- `build.sh` exports `CFLAGS`/`CXXFLAGS`/`LDFLAGS` only *after* libxml2, libpg_query and libzip are
  already built. Putting the flag there would build PHP for wasm64 against wasm32 dependency
  archives, which fails at `wasm-ld` with a machine-type mismatch — long after the mistake.
- `EMCC_CFLAGS` is appended by `emcc` itself to every invocation, so it reaches all three
  dependencies uniformly regardless of whether they are driven by autotools, cmake or a plain
  Makefile.

Two consequences worth knowing before changing any of this:

- **Emscripten's sysroot is per-target.** wasm64 system libraries and ports are generated into
  `sysroot/lib/wasm64-emscripten/` alongside the wasm32 ones, so the Emscripten cache does **not**
  need clearing when switching targets. But the zlib port path must follow the target — hence
  `$EM_TARGET` in `build.sh` rather than a hardcoded `wasm32-emscripten`. Hardcoding it finds a real
  file of the *wrong* architecture, which the existing `[ ! -f ]` guard cannot detect.
- **Switching target does not invalidate the dependency guards.** Each guard tests its own build
  output (`libxml2-2.11.4/` the source dir, `libzip-1.11.3/install/lib/libzip.a`,
  `libpg_query-18-latest/libpg_query.a`), and none of them knows about the target. libzip
  additionally caches the resolved zlib path in `build/CMakeCache.txt`, which is why `build.sh`
  removes that directory before configuring.

### The wasm memory leak, and why the page no longer has a Run budget

This is resolved, but the mechanism is worth keeping written down: it is a genuine Emscripten/php-src
interaction, it is invisible without a diagnostic that used to be compiled out, and anything that
regresses the lifecycle or Opcache brings it straight back.

**The symptom.** A tab executed a fixed number of Runs and then stopped executing PHP at all — every
later Run returned empty output, including `<?php echo 1;`, with no error anywhere. Only a reload
recovered it. Memory grew monotonically to `MAXIMUM_MEMORY` and, because WebAssembly memory can never
shrink, none of it ever came back:

```
run 1..45   128 MB -> 221 -> 459 -> 745 -> ... -> 2019
run 46      2048 MB = MAXIMUM_MEMORY exactly
            Fatal error: Out of memory (allocated 8388608 bytes) (tried to allocate 32768 bytes)
run 47+     RangeError: Maximum call stack size exceeded   <- the silent state
```

PHP's own heap was only 8 MB at that point, so `memory_limit` was irrelevant — the exhausted resource
was the wasm heap, held outside PHP's request allocator. `MAXIMUM_MEMORY` cannot be raised under
`MEMORY64=2`, which lowers to a 32-bit memory.

**The cause.** `zend_mm_chunk_alloc_int()` (`Zend/zend_alloc.c`) aligns a 2 MB chunk by mapping
`size + alignment - REAL_PAGE_SIZE` and then `munmap`ing the unwanted head and tail. Emscripten's
`munmap` cannot do that: `__syscall_munmap` looks the address up in its mapping list and returns
`EINVAL` unless the length matches the *whole* recorded mapping. So both trims fail and roughly one
`ZEND_MM_CHUNK_SIZE` of alignment slack is orphaned on every chunk allocation, permanently.

`wasm/patches/php-8.5-emscripten-mm-chunk-alignment.patch` fixes it at the source: under
`__EMSCRIPTEN__` the chunk comes from `posix_memalign()` and goes back via `free()`, so there is
nothing to trim; `chunk_truncate`/`chunk_extend` report failure, which is the branch `_WIN32` has
always taken and which callers already handle by falling back to allocate-copy-free.

**Why it took so long to find.** `build.sh` used to compile with `-DZEND_MM_ERROR=0`, which suppresses
the `munmap() failed: [%d] %s` that this code prints on exactly this failure (`Zend/zend_alloc.c`).
That flag is gone. Errno `28` is Emscripten's `EINVAL` — its errno table is not glibc's, so the
number looks like `ENOSPC` while the text reads "Invalid argument". **Do not re-add
`-DZEND_MM_ERROR=0`**; it costs the only signal this failure produces.

**What each change bought**, measured over the same rotation of five real pipelines:

| build | leak per Run | Runs before death |
|---|---|---|
| module lifecycle per Run, no Opcache | 36.2 MB | 53 |
| request lifecycle per Run, no Opcache | 2.36 MB | 813 |
| + Opcache with a working SHM backend | 0.00 MB | no death in 5,000 |

Opcache takes it to zero by removing the demand rather than fixing the bug — with the phar compiled
once into shared memory, PHP's heap never grows past its warm set, so chunks stop being allocated.
The allocator patch is what makes that flat curve structural instead of contingent on Opcache never
being outgrown or disabled.

**Measurement trap.** Wasm memory grows in ~98 MB steps, so per-Run figures from short samples are
quantization noise, not data — during the original investigation the same code measured 24.5 and then
16.3 MB/Run, and a curve that looked like it was converging at 382 MB turned out to be dead linear
over 600 Runs. Always report a total over >= 30 Runs, measure growth over the tail rather than the
whole span (early Runs carry one-off warm-up costs), and never call a plateau from fewer than a few
hundred flat Runs.

### The controller recycles the module as a backstop

`web/landing/assets/controllers/wasm_controller.js` reads the wasm heap size through the
`pib_heap_bytes()` export after each Run and, past a threshold, transparently tears the module down
and builds a fresh one, preserving `/workspace`.

With the lifecycle split, Opcache and the allocator patch all in place this should never fire. It
exists so that any future regression degrades into a brief re-initialisation instead of the silent
unrecoverable death described above — that failure mode cost a full investigation to diagnose once,
and it should not be able to reach a user again.

### Wrapper Code

The `pib_eval.c` file provides the C wrapper for evaluating PHP code. These three are the whole
JS-callable surface — `EXPORTED_FUNCTIONS` in `build.sh` lists exactly them:

- `pib_eval(char *code)` — evaluates PHP code and returns the result
- `pib_force_exit()` — forces exit from the WASM runtime
- `pib_heap_bytes()` — current wasm heap size, for the controller's recycling backstop. Returns a
  `double` rather than `size_t` because under `MEMORY64` a `size_t` return is an `i64`, which
  `ccall`'s `'number'` type cannot carry without BigInt handling on the JS side.

`php_embed_init`, `php_embed_shutdown` and `zend_eval_string` used to be exported too. They were
never called from JS, and calling them now would corrupt the module's lifecycle state, so they are
not exported any more.

## Troubleshooting

### Build fails with configure errors

Check `config.log` in the PHP directory for detailed error messages.

### Missing extensions

Ensure all required libraries (libxml2, libpg_query) are built before PHP configuration.

### Memory issues in browser

The playground may fail on very large datasets due to WASM memory limits. `MAXIMUM_MEMORY` is 2 GB
and cannot be raised — `MEMORY64=2` lowers to a 32-bit memory, so that ceiling is a property of the
mode, not a leftover setting.

A *steadily climbing* footprint across Runs is a different problem and a regression: memory should be
flat once the phar is warm. Check that Opcache is actually running
(`opcache_get_status(false)` must return an array, not `false`) before looking anywhere else — that
is the single thing keeping the curve flat.
