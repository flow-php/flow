---
seo_title: "Installing Arrow Extension"
seo_description: >
  How to install flow-php/arrow-ext PHP extension using precompiled binaries or PIE.
---

# Arrow Extension

[DOC_LINK:/documentation/components/extensions/arrow-ext.md]

- [📜 Documentation](/documentation/components/extensions/arrow-ext.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/arrow-ext)

[TOC]

## Precompiled Binaries

Precompiled binaries are available for download from the [GitHub Releases](https://github.com/flow-php/arrow-ext/releases/latest) page.

Binary names follow the [PIE naming convention](https://github.com/php/pie/blob/1.4.x/docs/extension-maintainers.md):

`php_arrow-{Version}_php{PhpVersion}-{Arch}-{OS}-{Libc}[-{TSMode}].zip`

Available platforms:
- macOS ARM64 (Apple Silicon)
- Linux ARM64
- Linux x86_64

PHP versions: 8.3, 8.4, 8.5 (each with NTS and ZTS variants).

### Installation

Download the matching zip for your platform, then:

```bash
# Unzip the archive
unzip php_arrow-*.zip

# Copy to your PHP extensions directory
cp arrow.so $(php -r "echo ini_get('extension_dir');")

# Enable it
echo "extension=arrow" > $(php -r "echo PHP_CONFIG_FILE_SCAN_DIR;")/arrow.ini

# Verify
php -m | grep arrow
```

## PIE

[PIE](https://github.com/php/pie) is the modern PHP extension installer.

```bash
pie install flow-php/arrow-ext
```

## Build Prerequisites

The Arrow extension is written in Rust and requires the following tools to compile:

- **Rust toolchain** (rustc, cargo) - install via [rustup](https://rustup.rs/)
- **clang** - required by the Rust build process
