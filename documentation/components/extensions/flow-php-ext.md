---
package: flow-php/flow-php-ext
---

# Flow PHP Extension

[PACKAGE_NAV]

[TOC]

This extension is the native column backend of Flow. It uses [ext-php-rs](https://github.com/extphprs/ext-php-rs)
to read and write CSV and JSON natively in Rust. It reads and writes Parquet through the
[arrow extension](/documentation/components/extensions/arrow-ext.md). The pure-PHP implementations in `flow-php/etl`
are the canonical reference for the behaviour, and they work without the extension. The extension only makes Flow
faster.

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

The CSV reader uses the extension implicitly:

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{df, to_stream};

df()
    ->read(from_csv(__DIR__ . '/input.csv')) // read natively when the extension is loaded
    ->write(to_stream(__DIR__ . '/output.txt'))
    ->run();
```

`Flow\ETL\Values\Column\AdaptiveBackend` is the default of `config_builder()->backend()`. When the extension is loaded,
it picks the `RustBackend` of the extension. Each batch column is then a `Flow\ETL\Values\Column\RustColumn` over an
Apache Arrow array. To choose a backend and to see what it builds, read
[Column Backend](/documentation/components/core/column-backend.md).

```php
<?php

use Flow\ETL\Values\Column\RustBackend;

use function Flow\ETL\DSL\int_schema;

$builder = (new RustBackend())->builder(int_schema('id'));
$builder->appendMany([1, '2', 3.0]);

$column = $builder->finish();                        // Flow\ETL\Values\Column\RustColumn
$column->values();                                   // [1, 2, 3]
(new RustBackend())->allocatedBytes();               // bytes held outside PHP's memory manager
```

Each contract with a Rust and a PHP implementation follows one pattern:

- The extension registers the interface.
- The `Rust*` class implements it.
- `Php*` is the PHP implementation.
- The class that builds the contract picks one of them with `extension_loaded()`.

| contract                                                  | Rust                    | PHP                    | picked by                            |
|-----------------------------------------------------------|-------------------------|------------------------|--------------------------------------|
| `Flow\ETL\Values\Column\Backend`                          | `RustBackend`           | `PhpBackend`           | `AdaptiveBackend`                    |
| `Flow\ETL\Adapter\CSV\IO\Extractor\CSVOpenSource`         | `RustCSVOpenSource`     | `PhpCSVOpenSource`     | `CSVSourceOpener`                    |
| `Flow\ETL\Adapter\CSV\IO\Loader\CSVEncoder`               | `RustCSVEncoder`        | `PhpCSVEncoder`        | `CSVLoader::writer()`                |
| `Flow\ETL\Adapter\JSON\IO\Extractor\JsonOpenSource`       | `RustJsonOpenSource`    | `PhpJsonOpenSource`    | `JsonFileBatches::open()`            |
| `Flow\ETL\Adapter\JSON\IO\Loader\JsonEncoder`             | `RustJsonEncoder`       | `PhpJsonEncoder`       | `JsonLoader::writer()`               |
| `Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetOpenSource` | `RustParquetOpenSource` | `PhpParquetOpenSource` | `ParquetSourceFile::open()`          |
| `Flow\ETL\IO\Loader\File\FileWriter`                      | `RustParquetFileWriter` | `ParquetFileWriter`    | `ParquetFileWriterFactory::writer()` |

The extension also registers `Flow\ETL\Values\Column\{Column, ColumnBuilder}` (`RustColumn`, `RustColumnBuilder`) and
`Flow\ETL\Values\RustIterator`, the iterator its sources return.

## CSV and JSON writers

When the extension is loaded, `to_csv()`, `to_json()` and `to_json_lines()` render a batch from the arrow buffers,
without a PHP value for each cell. The bytes are the same as the bytes of the PHP writers.

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

The native renderer accepts a datetime or date format that uses only the letters
`Y y m n d j H G i s u v e T P p O Z U c` and `\` escapes. If a format has a different letter, that column renders in
PHP. These columns also render in PHP:

- `xml`, `xml_element`, `html` and `html_element` columns
- a `json` column written to JSON
- a `list` / `map` / `structure` that holds one of those

Only those columns render in PHP. The rest of the batch stays native. `to_json()` and `to_json_lines()` render
natively with the flags `JSON_THROW_ON_ERROR`, `JSON_UNESCAPED_SLASHES`, `JSON_UNESCAPED_UNICODE` and
`JSON_PRESERVE_ZERO_FRACTION`. With a different flag (`JSON_PRETTY_PRINT`, ...), they write through PHP.

`RustCSVEncoder` / `RustJsonEncoder` hold the PHP encoder that renders the PHP columns.

## Parquet

When this extension and the [arrow extension](/documentation/components/extensions/arrow-ext.md) are both loaded,
`from_parquet()` and `to_parquet()` without an `engine:` read and write native columns. arrow-ext reads and writes the
file. Its batches go into `RustColumn`s (and back) through the Arrow C Data Interface. There is no copy and no PHP
value for each cell.

| loaded          | `from_parquet()` / `to_parquet()`                                           |
|-----------------|-----------------------------------------------------------------------------|
| flow_php, arrow | native columns through the Arrow C Data Interface                           |
| arrow           | `RustParquetEngine` (arrow-ext), PHP values                                 |
| flow_php        | `PhpParquetEngine`, and `RustBackend` adopts the values into native columns |
| neither         | `PhpParquetEngine`                                                          |

```php
<?php

use Flow\Parquet\Engine\PhpParquetEngine;

use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\df;

df()
    ->read(from_parquet(__DIR__ . '/orders.parquet'))
    ->offset(50_000) // only the row groups from row 50 000 are decoded
    ->write(to_parquet(__DIR__ . '/copy.parquet'))
    ->run();

df()
    ->read(from_parquet(__DIR__ . '/orders.parquet', engine: new PhpParquetEngine())) // PhpParquetEngine opts out
    ->write(to_parquet(__DIR__ . '/copy.parquet', engine: new PhpParquetEngine()))
    ->run();
```

A schema type can store a different arrow type than the type that the file column reads as. The read refuses such a
type before the first batch. `RustBackend::allocatedBytes()` counts the imported batches while they live. It does not
count the read buffers of arrow-ext: the projected column chunks of a row group and the decode buffers. A
`MemoryBudget` therefore does not see them. Read and write refusals are the `Flow\Parquet\Exception\*` of arrow-ext,
as with `RustParquetEngine`.

`RustParquetOpenSource` reads through `Flow\Arrow\Parquet\RustBatchReader` of arrow-ext. `RustParquetFileWriter` writes
through `Flow\Parquet\Engine\RustParquetFileWriter` of arrow-ext.

