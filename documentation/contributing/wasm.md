# WASM - Interactive Playground Build

[TOC]

This page tells you how to build PHP to WebAssembly (WASM) for the Flow PHP Interactive Playground.

## Overview

The WASM build makes a PHP runtime that runs in the browser. The interactive playground at
[flow-php.com](https://flow-php.com) uses it. The build compiles PHP 8.5.12 with the extensions that Flow PHP ETL
pipelines need in the browser. The version is the same as in the Docker image (`Dockerfile` `FLOW_PHP_VERSION`), so
both distributions have the same PHP.

The build targets 64-bit wasm (`-sMEMORY64=2`), so the playground has the same `PHP_INT_MAX` as all other
distributions. For more information, read [64-bit Environment](#64-bit-environment) and
[Why MEMORY64=2 and not MEMORY64=1](#why-memory642-and-not-memory641).

## Development Setup

```bash
nix-shell --arg with-wasm true
```

This shell gives you all the dependencies, for example Emscripten, autoconf, cmake, wget and libxml2.

## Commands

Build the WASM binary:

```bash
nix-shell --arg with-wasm true --run "just wasm"
```

Use `just wasm`, not `wasm/build.sh` directly. The recipe also builds `flow.phar` again into
`web/landing/assets/wasm/tools/`, and the playground loads that phar. If you build only the runtime, the new PHP runs
with an old library.

The build script does these steps:

1. Download and compile libxml2 for WebAssembly
2. Download and compile libpg_query for WebAssembly
3. Download PHP source
4. Apply the php-src patches in `wasm/patches/`
5. Copy the pg_query and snappy extensions
6. Configure and compile PHP with required extensions
7. Link everything into `php.wasm` and `php.js`
8. Copy outputs to `web/landing/assets/wasm/`

The script skips each dependency that already has its build output, so a second run is fast. A new version changes
the directory name, which starts the build again. For libpg_query and libzip, a new version makes the archive check
fail, which also starts the build again.

### libpg_query version

`build.sh` does not pin its own libpg_query version. It reads `LIBPG_QUERY_VERSION` from
`src/extension/pg-query-ext/Makefile`. The playground and the native extension therefore always use the same
PostgreSQL grammar. If the script cannot read that variable, it stops immediately.

The pin is a release tag (`18.0.0`), so each build is reproducible. If you change the pin, the script clones the new
version.

## Included PHP Extensions

| Extension | Purpose                                                              |
|-----------|----------------------------------------------------------------------|
| bcmath    | Required by flow-php/parquet for decimal handling                    |
| mbstring  | String handling                                                      |
| phar      | PHAR archive support                                                 |
| filter    | Data filtering                                                       |
| tokenizer | PHP tokenization                                                     |
| zlib      | Compression support                                                  |
| iconv     | Character encoding conversion                                        |
| libxml    | Base for XML extensions                                              |
| xml       | XML parsing                                                          |
| dom       | DOM manipulation                                                     |
| xmlreader | XML streaming reader                                                 |
| xmlwriter | XML streaming writer                                                 |
| zip       | Required by flow-php/etl-adapter-excel (XLSX files are ZIP archives) |
| pg_query  | PostgreSQL query parsing (Flow PHP extension)                        |
| snappy    | Snappy compression for Parquet                                       |

## Output Files

| File           | Description                 |
|----------------|-----------------------------|
| `out/php.wasm` | Compiled WebAssembly binary |
| `out/php.js`   | JavaScript loader/glue code |

The build copies these files to `web/landing/assets/wasm/`, where the playground uses them.

## Architecture Notes

### Opcache is always compiled in, and is deliberately switched on

In PHP 8.5, Opcache is not optional
([RFC](https://wiki.php.net/rfc/make_opcache_required), php-src `7b4c14dc1016`). The
`PHP_ARG_ENABLE([opcache], ...)` that `--disable-all` used to turn off is gone. `config.m4` now always calls
`PHP_NEW_EXTENSION([opcache], ...)`. On 8.4, `--disable-all` removed Opcache without a message. For this reason,
nobody wrote this down before.

Because of this change, the build uses two configure flags. Neither flag is redundant next to `--disable-all`. Both
give `[no]` as their 5th `PHP_ARG_ENABLE` argument, so `--disable-all` cannot reach them:

| Flag                        | Why                                                                                                                                                                                                                                                                                                                                                                                       |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--disable-opcache-jit`     | The JIT of Opcache depends on `$host_cpu`. `emconfigure` does not give `--host`, so `config.guess` reports the build machine. `config.m4` accepts all common machines (`x86*`, `aarch64`, `amd64`) as JIT-capable. Without this flag, `emcc` gets `jit/ir/*.c` and dynasm for the host ISA to compile into a wasm32 binary. This is the same type of workaround as `--disable-fiber-asm`. |
| `--disable-huge-code-pages` | A copy of code pages into huge pages has no meaning under wasm.                                                                                                                                                                                                                                                                                                                           |

Do not delete `--disable-opcache-jit` because it looks redundant. It is not redundant.

#### The shared-memory backend has to be forced on

Without a change, Opcache builds but can never run. Each of its three shared-memory probes compiles a program that
calls `fork()`. Emscripten does not implement `fork()`, so all three probes fail, and configure defines
`NO_SHM_BACKEND`:

```
checking for sysvipc shared memory support... no
checking for mmap() using MAP_ANON shared memory support... no
checking for mmap() using shm_open() shared memory support... no
configure: WARNING: No supported shared memory caching support was found when configuring opcache.
Opcache will be disabled.
```

Since php-src `e4078a6a70d5`, this is a warning and not a build failure. At runtime, `NO_SHM_BACKEND` disables
Opcache, and PHP still starts. Each backend source (`shared_alloc_mmap.c`, `shared_alloc_posix.c`,
`shared_alloc_shm.c`) is fully inside `#ifdef USE_MMAP` / `USE_SHM_OPEN` / `USE_SHM`. If none of them is defined, the
sources compile to nothing.

`build.sh` therefore sets the autoconf cache variable before `./configure`:

```bash
export php_cv_shm_mmap_anon=yes
```

The probe then shows `checking for mmap() using MAP_ANON shared memory support... (cached) yes`, and configure
defines `HAVE_SHM_MMAP_ANON` → `USE_MMAP`. The script forces only that one. The other two stay `no`. The three checks
are siblings, not nested (`ext/opcache/config.m4:176-234`), and `USE_MMAP` alone is sufficient.

This is not a lie to the build system. The probe fails only on its `fork()` part. Opcache gets all the parts that it
needs at runtime:

- `mmap(MAP_SHARED|MAP_ANONYMOUS)`: the `__syscall_mmap2` of Emscripten takes an anonymous branch. That branch
  ignores `MAP_SHARED` and gives the mapping from linear memory (`system/lib/libc/emscripten_mmap.c`). A runtime
  with one process needs no real sharing.
- `fcntl(F_SETLK/F_SETLKW)`: Emscripten returns success by design. Its comment says that these are process-level
  locks and that a wasm program is one process (`src/lib/libsyscall.js`).
- The lock file: there is no `memfd_create` or `O_TMPFILE`, so the code uses `mkstemp()` under
  `opcache.lockfile_path`. MEMFS gives `/tmp` by default.

All three were tested with a standalone `emcc` program before the PHP build, not found after it. Do this test again
after each toolchain update.

#### Why this reverses the earlier decision

An earlier version of this file recorded the opposite decision. Opcache stayed inactive. The reason was that the
playground evaluates a new snippet on each run. For this reason, an opcode cache seemed to give no benefit. The first part is true, but the
second part does not follow from it. The snippet is new for each Run. The 3.7 MB `flow.phar` behind it is not, and
the compilation of that class graph on each Run was the full cost. The measurements, with one PHP request for each
Run:

|                                    | leak per Run | Runs before the page died |
|------------------------------------|--------------|---------------------------|
| no Opcache                         | 2.36 MB      | 813                       |
| Opcache with a working SHM backend | 0.00 MB      | no death in 5,000         |

It is also 28% faster, not slower. The phar is no longer compiled on each Run. A CSV → JSON → read-back workload therefore went from a
319 ms median to a 228 ms median. The artifact grew 0.03%.

Opcache helps only together with the one-module-per-page lifecycle (read the next section). Its shared memory is
created at `MINIT`. Under the old one-module-per-Run model, the build made it again on each Run. This is very
probably why it looked useless.

#### Settings, and why they are set in C

PHP reads `opcache.memory_consumption` once at `MINIT`, so it must be set before `php_embed_init()`.
`php_embed_init()` overwrites `php_embed_module.ini_entries` with its own `HARDCODED_INI`. The only hook that you can
use is therefore `php_embed_module.ini_defaults`, and `pib_eval.c` sets it. This build has no `php.ini`.

`opcache.validate_timestamps=0`: the loader writes the phar once, and the phar does not change while the page is
open. The one file that changes (`code.php`, which the Format action writes again) is never `require`d. Snippets
are compiled through `zend_compile_string`, and Opcache does not cache them in any case. A `stat()` of each cached
file on each request therefore gives no benefit.

The SHM is `mmap`ed out of wasm linear memory at `MINIT` and is never returned. `opcache.memory_consumption` is
therefore a permanent cost of each page for each visitor, also for visitors who never push Run. Set its size from
measurements, not from the upstream default.

`opcache.enable_cli` has no effect here. `accel_sapi_is_cli()` matches only `cli` and `phpdbg`, and the name of this
SAPI is `embed`.

### One module lifecycle per page, one request lifecycle per Run

`pib_eval()` (`wasm/pib_eval.c`) calls `php_embed_init()` once, on the first Run of a page. After that, it only
stops and starts the request around each evaluation:

```
first Run only :  php_embed_init()          =  sapi_startup -> MINIT -> php_request_startup
every Run      :  php_request_shutdown() ; php_request_startup()
never          :  php_embed_shutdown()
```

The embed SAPI splits along exactly those lines (`sapi/embed/php_embed.c`). All web SAPIs work like this. The old
model, with a full `php_embed_init()`/`php_embed_shutdown()` on each Run, was the unusual one. It cost the page
~36 MB of wasm memory on each Run, and the page did not get this memory back.

Each Run stays isolated. User-defined classes, functions, constants and globals are all request-scoped, and request
shutdown still frees them. Each case was tested against the old artifact, and all of them behave as before:

- class and function redeclaration
- `$GLOBALS` and `static` variables
- `ini_set` and `declare(strict_types=1)`
- autoloader registrations and open file handles
- `exit()`, `register_shutdown_function` and an uncaught throwable
- output from one Run in the next Run

Two details of the order are necessary:

- The request stops at the end of `pib_eval()`, not at the start of the next call. Shutdown functions and
  destructors then belong to the Run that caused them.
- `php_request_shutdown()` runs before the `fflush()` pair, because it writes the
  `register_shutdown_function` output.

Both `php_request_startup()` and `php_request_shutdown()` put their bodies in `zend_try` internally. Neither can
therefore bail out past the own `zend_first_try` of `pib_eval()`.

`php_embed_shutdown()` is now never called. A browser page has no teardown hook, and the recycling path of the
controller (read below) discards the full wasm instance, which frees all memory. This is deliberate. A call to
`php_embed_shutdown()` runs `MSHUTDOWN`, and the next section tells why that is a risk to avoid.

#### `MSHUTDOWN` now runs at most once - do not take that as licence

Under the old model, a `MINIT`/`MSHUTDOWN` pair that was not safe to run twice was fatal. The failure also did not point
to the cause. Run 1 succeeded, and Run 2 died with `null function` or `memory access out of bounds`. That was a wasm
indirect call through a freed function pointer, with no PHP stack trace.

This happened once. The `MSHUTDOWN` of `pg_query` called `pg_query_exit()`, which frees the `TopMemoryContext` of
libpg_query. `pg_query_init()` checks a flag that it never resets, so the second startup ran on freed memory. The fix
was to not free the memory at all (read the comment in `src/extension/pg-query-ext/ext/pg_query.c`). A reset of the
flag is not sufficient. `MemoryContextInit()` then runs again over freed contexts. A small snippet survives this,
but a real pipeline fails.

With the lifecycle change, this type of bug is much harder to hit. It is not a reason to restore
`pg_query_exit()`. That is a separate decision with its own history, and the recycling path still makes new modules
in one page.

When you add an extension to this build, still test it with a realistic workload: load the phar and run a pipeline
across many Runs. A `<?php echo 1;` probe does not find a request-scoped leak.

### php-src patches

The build applies `wasm/patches/*.patch` to the extracted PHP source after the download and before `buildconf`.
Each patch is applied only once. If the reverse patch applies cleanly, the patch is already there. This is necessary
because `build.sh` uses the same extracted source tree again across runs.

`build.sh` reads all files in the directory, so a new file is sufficient. There is no list to keep up to date.

| Patch                                         | Why                                                                                                                                                                                                                                                                                                                                   |
|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `php-8.5-opcache-unistd.patch`                | `ext/opcache/zend_accelerator_debug.c` calls `getpid()` in its non-ZTS branch, but it includes `<process.h>` only under `ZEND_WIN32`. Since version 16, clang treats implicit function declarations as an error, so the build fails without this patch. On 8.4, this problem did not show, because the build never compiled the file. |
| `php-8.5-emscripten-mm-chunk-alignment.patch` | The chunk allocator of Zend aligns memory with an allocation that is too large, followed by a partial `munmap`. Emscripten cannot do this, so each chunk lost ~2 MB permanently. For more information, read [the memory leak section](#the-wasm-memory-leak-and-why-the-page-no-longer-has-a-run-budget).                             |

After the next PHP update, look at both patches again. If PHP itself contains the fix, delete the patch.

### 64-bit Environment

The playground is 64-bit. `PHP_INT_MAX === 9223372036854775807` and `PHP_INT_SIZE === 8`. This was tested in the
browser, not inferred from build flags. This section described the opposite before. The history is important,
because the workarounds for the old state are still in the codebase.

These statements are now true:

- `pack()`/`unpack()` 64-bit format codes (`q`, `Q`, `J`, `P`) work. `Flow\Floe` therefore works. `sortBy()` also
  works, and so does each operation that spills runs to disk through
  `Flow\ETL\Values\Column\Layout\Int64Layout` (`pack('P*')`). Before this change, each Floe write in the browser
  died with `ValueError: 64-bit format codes are not available for 32-bit versions of PHP`.
- Large integers from Thrift/Parquet metadata do not overflow to float. On the 32-bit build, a read of a parquet
  file wrote hundreds of `Warning: The float 2147483648 is not representable as an int` from
  `Flow\Parquet\Thrift\CompactProtocol` and `Flow\ETL\Kernels\Bucketing\HashBucketing`. The 64-bit build writes
  none.
- Hex constants such as `0x80000000` are plain integers and need no `(int)` cast.
- `DateTimePhysical` stores `getTimestamp() * 1_000_000 + µs`, which now represents dates after 2038-01-19.

The `(int)` casts in the `fromThrift()` methods of the parquet library stay. The playground does not need them now.
But the library still supports 32-bit PHP builds, and their removal is a separate decision.

### Why `MEMORY64=2` and not `MEMORY64=1`

`build.sh` builds with `-sMEMORY64=2` (`WASM64_MODE`). Emscripten describes the three values like this:

- `0`: wasm32.
- `1`: full end-to-end wasm64.
- `2`: wasm64 for clang/lld, lowered to wasm32 by Binaryen.

Only `2` gives PHP an 8-byte `zend_long` without Memory64 support in the engine that runs it:

|                                      | `MEMORY64=1` | `MEMORY64=2`                     |
|--------------------------------------|--------------|----------------------------------|
| `sizeof(long)`                       | 8            | 8                                |
| Engine requirement                   | Memory64     | runs on today's wasm32 engines   |
| Node needed to run build-time probes | ≥ 23         | works on the nix shell's node 22 |

The node requirement is important. Under `emconfigure`, the PHP `configure` reports `checking whether we are cross
compiling... no` and runs its `AC_RUN_IFELSE` probes through node. With `=1`, the PHP configure fails or detects the
wrong values before it reaches other steps. Also, flow-php.com is public, so `=1` also needs a test of Memory64 in
Safari and Firefox.

`=2` is sufficient, because the playground does not need an address space larger than 4 GB.
`MAXIMUM_MEMORY=2147483648` shows this. The memory settings are deliberately the same as in the wasm32 build. `=2`
lowers to a 32-bit memory, so the 2 GB maximum is a hard limit of the mode, not an old setting. A larger maximum
needs `=1`.

A change to `=1` is a deliberate decision, not a tuning step.

### The target flag has to reach the dependencies, not just PHP

`build.sh` exports `WASM64_MODE` as `EMCC_CFLAGS`, near the top of the script. This location is necessary:

- `build.sh` exports `CFLAGS`/`CXXFLAGS`/`LDFLAGS` only after it built libxml2, libpg_query and libzip. If you put
  the flag there, PHP builds for wasm64 against wasm32 dependency archives. `wasm-ld` then fails with a machine-type
  mismatch, long after the mistake.
- `emcc` itself appends `EMCC_CFLAGS` to each call. The flag therefore reaches all three dependencies in the same
  way. Autotools, cmake and a plain Makefile all get it.

Know these two results before you change this:

- The sysroot of Emscripten is different for each target. Emscripten generates the wasm64 system libraries and ports
  into `sysroot/lib/wasm64-emscripten/`, next to the wasm32 ones. A change of the target therefore does not need a clear
  Emscripten cache. But the zlib port path must follow the target. For this reason, `build.sh` uses
  `$EM_TARGET` and not a hardcoded `wasm32-emscripten`. A hardcoded path finds a real file of the wrong
  architecture, and the existing `[ ! -f ]` guard cannot detect this.
- A change of the target does not reset the dependency guards. Each guard tests its own build output
  (`libxml2-2.11.4/` the source dir, `libzip-1.11.3/install/lib/libzip.a`, `libpg_query-18.0.0/libpg_query.a`), and
  no guard knows the target. libzip also caches the resolved zlib path in `build/CMakeCache.txt`. For this reason,
  `build.sh` removes that directory before it runs configure.

### The wasm memory leak, and why the page no longer has a Run budget

This problem is solved, but this section keeps a record of the mechanism, for three reasons:

- It is a real interaction between Emscripten and php-src.
- You cannot see it without a diagnostic that the build removed before.
- A regression in the lifecycle or in Opcache brings it back immediately.

The symptom: a tab ran a fixed number of Runs, and then it stopped to run PHP at all. Each later Run returned
empty output, also `<?php echo 1;`, and showed no error. Only a reload of the page helped. Memory grew all the time up
to `MAXIMUM_MEMORY`. WebAssembly memory can never become smaller, so none of this memory came back:

```
run 1..45   128 MB -> 221 -> 459 -> 745 -> ... -> 2019
run 46      2048 MB = MAXIMUM_MEMORY exactly
            Fatal error: Out of memory (allocated 8388608 bytes) (tried to allocate 32768 bytes)
run 47+     RangeError: Maximum call stack size exceeded   <- the silent state
```

At that point, the heap of PHP was only 8 MB, so `memory_limit` had no effect. The exhausted resource was the wasm
heap, outside the request allocator of PHP. You cannot raise `MAXIMUM_MEMORY` under `MEMORY64=2`, which lowers to a
32-bit memory.

The cause: `zend_mm_chunk_alloc_int()` (`Zend/zend_alloc.c`) aligns a 2 MB chunk. It maps
`size + alignment - REAL_PAGE_SIZE`, and then it `munmap`s the unwanted head and tail. The `munmap` of Emscripten
cannot do that. `__syscall_munmap` finds the address in its mapping list. The length must match the full recorded mapping.
Otherwise, it returns `EINVAL`. Both trims therefore fail. On each chunk allocation, approximately one
`ZEND_MM_CHUNK_SIZE` of alignment space is lost permanently.

`wasm/patches/php-8.5-emscripten-mm-chunk-alignment.patch` fixes the source of the problem. Under `__EMSCRIPTEN__`,
the chunk comes from `posix_memalign()` and goes back through `free()`, so there is nothing to trim.
`chunk_truncate`/`chunk_extend` report failure. `_WIN32` always takes this branch, and the callers already handle it:
they allocate, copy and free.

Why the search took so long: `build.sh` compiled with `-DZEND_MM_ERROR=0` before. This flag hides the
`munmap() failed: [%d] %s` message that this code writes on exactly this failure (`Zend/zend_alloc.c`). That flag is
gone now. Errno `28` is `EINVAL` in Emscripten. Its errno table is not the table of glibc, so the number looks like
`ENOSPC`, while the text says "Invalid argument". Do not add `-DZEND_MM_ERROR=0` again. It removes the only signal of
this failure.

The effect of each change, measured over the same rotation of five real pipelines:

| build                                 | leak per Run | Runs before death |
|---------------------------------------|--------------|-------------------|
| module lifecycle per Run, no Opcache  | 36.2 MB      | 53                |
| request lifecycle per Run, no Opcache | 2.36 MB      | 813               |
| + Opcache with a working SHM backend  | 0.00 MB      | no death in 5,000 |

Opcache brings the leak to zero because it removes the demand. It does not fix the bug. Opcache compiles the phar
once into shared memory, so the heap of PHP never grows past its warm set, and PHP stops the chunk allocations. The
allocator patch keeps the curve flat in all cases. A full or disabled Opcache does not change this.

Measurement trap: wasm memory grows in steps of ~98 MB. Numbers for each Run from short samples are therefore
quantization noise, not data. In the first investigation, the same code measured 24.5 MB/Run and then 16.3 MB/Run. A
curve that looked like it converged at 382 MB was fully linear over 600 Runs. Follow these rules:

- Always report a total over >= 30 Runs.
- Measure the growth over the tail, not over the full span. The early Runs have one-time warm-up costs.
- Never report a plateau from fewer than a few hundred flat Runs.

### The controller recycles the module as a backstop

`web/landing/assets/controllers/wasm_controller.js` reads the wasm heap size through the `pib_heap_bytes()` export
after each Run. Past a threshold, it stops the module and makes a new one, and it keeps `/workspace`. The user does
not see this.

With the lifecycle split, Opcache and the allocator patch in place, this never fires. It exists to turn each future
regression into a short initialisation, not into the silent death that the section above describes. That failure
needed a full investigation once, and it must not reach a user again.

### Wrapper Code

The `pib_eval.c` file contains the C wrapper that evaluates PHP code. These three functions are all that JS can
call. `EXPORTED_FUNCTIONS` in `build.sh` lists exactly them:

- `pib_eval(char *code)`: evaluates PHP code and returns the result.
- `pib_force_exit()`: forces an exit from the WASM runtime.
- `pib_heap_bytes()`: the current wasm heap size, for the recycling backstop of the controller. It returns a `double`
  and not `size_t`. Under `MEMORY64`, a `size_t` return is an `i64`. The `'number'` type of `ccall` cannot carry an
  `i64` without BigInt handling on the JS side.

`php_embed_init`, `php_embed_shutdown` and `zend_eval_string` were exported before. JS never called them, and a call
now corrupts the lifecycle state of the module. For this reason, the build does not export them now.

## Troubleshooting

### Build fails with configure errors

Read `config.log` in the PHP directory for the full error messages.

### Missing extensions

Make sure that all required libraries (libxml2, libpg_query) are built before you configure PHP.

### Memory issues in browser

The playground can fail on very large datasets because of WASM memory limits. `MAXIMUM_MEMORY` is 2 GB, and you
cannot raise it. `MEMORY64=2` lowers to a 32-bit memory, so that maximum is a property of the mode, not an old
setting.

A footprint that grows on each Run is a different problem, and it is a regression. After the phar is warm, memory
must stay flat. Before you look anywhere else, make sure that Opcache runs: `opcache_get_status(false)` must return
an array, not `false`. Opcache is the one thing that keeps the curve flat.
