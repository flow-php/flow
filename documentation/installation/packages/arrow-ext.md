---
seo_title: "Installing Arrow Extension"
seo_description: >
  How to install flow-php/arrow-ext PHP extension using PIE.
---

# Arrow Extension

- [⬅️️ Back](/documentation/installation.md)
- [📜 Documentation](/documentation/components/extensions/arrow-ext.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/arrow-ext)

[TOC]

## PIE

[PIE](https://github.com/php/pie) is the modern PHP extension installer.

```bash
pie install flow-php/arrow-ext
```

## Build Prerequisites

The Arrow extension is written in Rust and requires the following tools to compile:

- **Rust toolchain** (rustc, cargo) - install via [rustup](https://rustup.rs/)
- **clang** - required by the Rust build process
