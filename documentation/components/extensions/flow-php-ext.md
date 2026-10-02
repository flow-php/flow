---
package: flow-php/flow-php-ext
---

# Flow PHP Extension

[PACKAGE_NAV]

[TOC]

This extension is Flow's native column backend and reads CSV and JSON and writes CSV and JSON natively in Rust - Parquet
through the [arrow extension](/documentation/components/extensions/arrow-ext.md) - via
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

## CSV and JSON writers

With the extension loaded, `to_csv()`, `to_json()` and `to_json_lines()` render a batch from the arrow buffers, without
a PHP value per cell. The bytes are the ones the PHP writers produce.

```php
<?php

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\Adapter\JSON\to_json_lines;
use function Flow\ETL\DSL\df;

df()
    ->read(from_csv(__DIR__ . '/input.csv'))
    ->write(to_csv(__DIR__ . '/output.csv')->withDateTimeFormat('Y-m-d H:i:s.u P')) // rendered natively
    ->write(to_json_lines(__DIR__ . '/output.jsonl')->withDateTimeFormat('D, d M Y'))  // `D` and `M`: this column renders in PHP
    ->run();
```

A datetime or date format is rendered natively when its letters are among `Y y m n d j H G i s u v e T P p O Z U c`
(and `\` escapes); any other letter renders that column in PHP. `xml`, `xml_element`, `html` and `html_element` columns,
a `json` column written to JSON, and a `list` / `map` / `structure` holding one of those also render in PHP - only
those columns, the rest of the batch stays native. `to_json()` and `to_json_lines()` render natively under the flags
`JSON_THROW_ON_ERROR`, `JSON_UNESCAPED_SLASHES`, `JSON_UNESCAPED_UNICODE` and `JSON_PRESERVE_ZERO_FRACTION`; any other
flag (`JSON_PRETTY_PRINT`, ...) writes through PHP.

The classes are `Flow\ETL\Adapter\CSV\NativeCSVWriter` and `Flow\ETL\Adapter\JSON\NativeJsonWriter`.

## Parquet

With both this extension and the [arrow extension](/documentation/components/extensions/arrow-ext.md) loaded,
`from_parquet()` and `to_parquet()` without an `engine:` read and write native columns: arrow-ext reads and writes the
file, and its batches cross into `NativeColumn`s (and back) through the Arrow C Data Interface, without a copy and
without a PHP value per cell.

| loaded          | `from_parquet()` / `to_parquet()`                                          |
|-----------------|----------------------------------------------------------------------------|
| flow_php, arrow | native columns through the Arrow C Data Interface                          |
| arrow           | `ArrowParquetEngine`, PHP values                                           |
| flow_php        | `PhpParquetEngine`; `DefaultBackend` adopts the values into native columns |
| neither         | `PhpParquetEngine`                                                         |

```php
<?php

use Flow\Parquet\Engine\PhpParquetEngine;

use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\df;

df()
    ->read(from_parquet(__DIR__ . '/orders.parquet')->withOffset(50_000)) // only the row groups from row 50 000 are decoded
    ->write(to_parquet(__DIR__ . '/copy.parquet'))
    ->run();

df()
    ->read(from_parquet(__DIR__ . '/orders.parquet', engine: new PhpParquetEngine())) // PhpParquetEngine opts out
    ->write(to_parquet(__DIR__ . '/copy.parquet', engine: new PhpParquetEngine()))
    ->run();
```

A schema type that stores another arrow type than the file column reads as is refused before the first batch. The
imported batches are counted by `DefaultBackend::allocatedBytes()` for as long as they live; arrow-ext's own read
buffers (a row group's projected column chunks, decode buffers) are not, so a `MemoryBudget` does not see them. Read and
write refusals are arrow-ext's `Flow\Parquet\Exception\*`, as with `ArrowParquetEngine`.

The extension registers `Flow\ETL\Adapter\Parquet\{NativeParquetReader, NativeParquetWriter}` (over
`Flow\Arrow\Parquet\BatchReader` / `RowsWriter`) and `Flow\ETL\Column\NativeArrowBatch`, the batch it exports.

