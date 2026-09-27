---
package: flow-php/flow-php-ext
---

# Flow PHP Extension

[PACKAGE_NAV]

[TOC]

This extension reads CSV and narrows string columns to their types natively in Rust via
[ext-php-rs](https://github.com/extphprs/ext-php-rs). The pure-PHP implementations in `flow-php/etl` are the canonical
behaviour reference and work without the extension - loading it is purely an optimization.

## Loading the Extension

### In php.ini

```ini
extension = flow_php
```

### During Development

```bash
php -d extension=./ext/modules/flow_php.so your_script.php
```

## Usage

The extension is used implicitly by the CSV reader:

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{df, to_stream};

df()
    ->read(from_csv(__DIR__ . '/input.csv')) // read natively when the extension is loaded
    ->write(to_stream(__DIR__ . '/output.txt'))
    ->run();
```

The extension registers `Flow\ETL\Adapter\CSV\RustCSVReaderNative` and
`Flow\ETL\Adapter\CSV\RustColumnFoldNative`, used by `Flow\ETL\Adapter\CSV\NativeCSVOpenSource` for CSV reading and
string-type narrowing. The Floe codec and the row hydrator run in PHP until the native column backend lands.
