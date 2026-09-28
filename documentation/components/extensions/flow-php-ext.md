---
package: flow-php/flow-php-ext
---

# Flow PHP Extension

[PACKAGE_NAV]

[TOC]

This extension is Flow's native column backend and reads CSV natively in Rust via
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

With the extension loaded, `Flow\ETL\Column\DefaultBackend` - the default of `config_builder()->backend()` - is the
extension's class and every batch column is a `Flow\ETL\Column\NativeColumn` over an Apache Arrow array:

```php
<?php

use Flow\ETL\Column\DefaultBackend;

use function Flow\ETL\DSL\int_schema;

$builder = (new DefaultBackend())->builder(int_schema('id'));
$builder->appendMany([1, '2', 3.0]);

$column = $builder->finish();                        // Flow\ETL\Column\NativeColumn
$column->values();                                   // [1, 2, 3]
(new DefaultBackend())->allocatedBytes();            // bytes held outside PHP's memory manager
```

The extension registers the interfaces `Flow\ETL\Column\{Backend, Column, ColumnBuilder}`, the classes
`Flow\ETL\Column\{DefaultBackend, NativeColumn, NativeColumnBuilder}`, and `Flow\ETL\Adapter\CSV\RustCSVReaderNative`
and `Flow\ETL\Adapter\CSV\RustColumnFoldNative`, which read CSV straight into native columns and narrow string
columns to their types.
