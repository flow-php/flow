---
package: flow-php/flow-php-ext
---

# Flow PHP Extension

[PACKAGE_NAV]

[TOC]

This extension is Flow's native column backend and reads CSV and Parquet and writes CSV, JSON and Parquet natively in Rust via
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

With the extension loaded, `from_parquet()` and `to_parquet()` without an `engine:` read and write Parquet inside the
extension: row groups become `NativeColumn`s, and `Rows` are written without a PHP value per cell.

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

`engine: new ArrowParquetEngine()` and `new AdaptiveParquetEngine()` read and write through the extension too. A column
type the extension has no cast for is refused before the first batch, naming the engine that reads it:

```
Parquet column "iv" (Interval(DayTime)) is not supported by the flow_php Parquet reader; read the file with
from_parquet($path, engine: new \Flow\Parquet\Engine\PhpParquetEngine())
```

A string that is not valid UTF-8 is refused on write, naming the column and its 0-based row in the batch
(`Parquet column "name" row 0 holds a string that is not valid UTF-8; Parquet STRING columns require UTF-8`). Everything the reader and writer allocate is counted by
`DefaultBackend::allocatedBytes()`.

The extension also registers `Flow\ETL\Adapter\Parquet\{NativeParquetReader, NativeParquetWriter}`, the reader and
writer `from_parquet()` and `to_parquet()` use.

### The parquet library

`Flow\Parquet\Reader::arrow()` / `Writer::arrow()` (`ArrowParquetEngine`) read and write through the extension: the
values are the ones `Reader::php()` returns, and writes accept and refuse what `Writer::php()` does. The footer is read
once and decoded in Rust; `schema()`, `rowsNumber()` and `totalByteSize()` never build the PHP metadata graph.

```php
<?php

use Flow\Parquet\Reader;

$file = Reader::arrow()->read(__DIR__ . '/orders.parquet');

$file->reader()->rowsNumber();                            // from the footer, no PHP metadata graph
$file->values(['address.city'], limit: 10, offset: 5_000); // a struct path; only the row groups from row 5 000
```

The classes behind it are `Flow\Parquet\Engine\Native\{NativeParquetFile, NativeParquetColumnsReader,
NativeParquetRowsWriter}`. `Writer::writeColumns()` appends each list straight to its column.
