---
package: flow-php/flow-php-ext
seo_title: "Installing Flow PHP Extension"
seo_description: >
  How to install flow-php/flow-php-ext PHP extension using precompiled binaries or PIE.
---

# Flow PHP Extension

[PACKAGE_NAV:install]

[TOC]

## Precompiled Binaries

Precompiled binaries are available for download from the [GitHub Releases](https://github.com/flow-php/flow-php-ext/releases/latest) page.

Binary names follow the [PIE naming convention](https://github.com/php/pie/blob/1.4.x/docs/extension-maintainers.md):

`php_flow_php-{Version}_php{PhpVersion}-{Arch}-{OS}-{Libc}[-{TSMode}].zip`

Available platforms:
- macOS ARM64 (Apple Silicon)
- Linux ARM64 — glibc (Debian, Ubuntu, …) and musl (Alpine)
- Linux x86_64 — glibc (Debian, Ubuntu, …) and musl (Alpine)

PHP versions: 8.3, 8.4, 8.5 (each with NTS and ZTS variants).

### Installation

Download the matching zip for your platform, then:

```bash
# Unzip the archive
unzip php_flow_php-*.zip

# Copy to your PHP extensions directory
cp flow_php.so $(php -r "echo ini_get('extension_dir');")

# Enable it
echo "extension=flow_php" > $(php -r "echo PHP_CONFIG_FILE_SCAN_DIR;")/flow_php.ini

# Verify
php -m | grep flow_php
```

## PIE

[PIE](https://github.com/php/pie) is the modern PHP extension installer.

```bash
pie install flow-php/flow-php-ext
```

### Alpine Linux (musl)

Alpine images need `libgcc` before the extension will load:

```bash
apk add --no-cache libgcc
pie install flow-php/flow-php-ext
```

`libgcc` (129 KiB) provides `libgcc_s.so.1`, which the Rust runtime links for stack unwinding. Debian and Ubuntu
ship it in the base system; Alpine does not. Without it PHP reports:

```
Unable to load dynamic library '.../flow_php.so'
  (Error loading shared library libgcc_s.so.1: No such file or directory)
```

Installing `libgcc` is still dramatically cheaper than the alternative — without a musl binary PIE falls back to a
source build, which pulls the full Rust toolchain (`rust cargo clang-dev`, ~824 MB) into the image.

## Build Prerequisites

The Flow PHP extension is written in Rust and requires the following tools to compile:

- **Rust toolchain** (rustc, cargo) - install via [rustup](https://rustup.rs/)
- **clang** - required by the Rust build process
