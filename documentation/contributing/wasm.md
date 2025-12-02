# WASM Build

[TOC]

This document describes how to build PHP to WebAssembly (WASM) for use in the Flow PHP Interactive Playground.

## Overview

The WASM build creates a browser-compatible PHP runtime that powers the interactive playground
at [flow-php.com](https://flow-php.com). It compiles PHP 8.4 with the necessary extensions to run Flow PHP ETL pipelines
directly in the browser.

## Requirements

- [Nix](https://nixos.org/download.html) (recommended)
- Or manually: [Emscripten SDK](https://emscripten.org/docs/getting_started/downloads.html) (emcc, emmake, emconfigure),
  standard build tools (make, gcc, autoconf, automake, libtool), wget, git

## Building

The recommended way is to use the Nix shell with the `with-wasm` argument:

```bash
# From the repository root
nix-shell --arg with-wasm true

# Then run the build script
cd wasm
./build.sh
```

This will provide all necessary dependencies including Emscripten, autoconf, wget, and libxml2.

Alternatively, if you have Emscripten installed manually:

```bash
cd wasm
./build.sh
```

The build script will:

1. Download and compile libxml2 for WebAssembly
2. Download and compile libpg_query for WebAssembly
3. Download PHP source
4. Copy the pg_query and snappy extensions
5. Configure and compile PHP with required extensions
6. Link everything into `php.wasm` and `php.js`
7. Copy outputs to `web/landing/assets/wasm/`

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
| pg_query  | PostgreSQL query parsing (Flow PHP extension)     |
| snappy    | Snappy compression for Parquet                    |

## Architecture Notes

### 32-bit Environment

WebAssembly runs as a 32-bit environment where `PHP_INT_MAX = 2147483647`. This affects:

- Large integer values from Thrift/Parquet metadata may overflow to floats
- The parquet library includes `(int)` casts in `fromThrift()` methods to handle this
- Hex constants like `0x80000000` and `0xFFFFFFFF` require explicit `(int)` casts

## Output Files

| File           | Description                 |
|----------------|-----------------------------|
| `out/php.wasm` | Compiled WebAssembly binary |
| `out/php.js`   | JavaScript loader/glue code |

These are copied to `web/landing/assets/wasm/` for use by the playground.

## Wrapper Code

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
