# Extension: Flow PHP

A Rust-powered PHP extension using [ext-php-rs](https://github.com/extphprs/ext-php-rs) providing native
implementations for performance-critical parts of the Flow DataFrame framework: encoding and decoding of the
Floe binary format frames and row hydration/casting against a schema. The extension is optional — the pure-PHP
implementations in `flow-php/etl` are the canonical behavior reference, and Flow routes to the native ones
automatically when the extension is loaded.

> [!IMPORTANT]  
> This repository is a subtree split from our monorepo. If you'd like to contribute, please visit our main monorepo [flow-php/flow](https://github.com/flow-php/flow).

- 📜 [Documentation](https://flow-php.com/documentation/components/extensions/flow-php-ext/)
- ➡️ [Installation](https://flow-php.com/documentation/installation/packages/flow-php-ext/)
- 🛠️ [Contributing](https://flow-php.com/documentation/contributing/)
- 🚧 [Upgrading](https://flow-php.com/documentation/upgrading/)
