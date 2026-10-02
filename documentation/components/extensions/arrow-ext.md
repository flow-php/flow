---
package: flow-php/arrow-ext
---

# Arrow Extension

[PACKAGE_NAV]

[TOC]

[Apache Arrow](https://arrow.apache.org/) is a language-independent columnar memory format for flat and hierarchical data.
The Arrow ecosystem provides high-performance implementations for common data operations - including I/O for formats like
Parquet, CSV, JSON, and Arrow IPC - in C++, Rust, Java, Python, and other languages.

This extension brings the [Arrow Rust ecosystem](https://github.com/apache/arrow-rs) into PHP via
[ext-php-rs](https://github.com/extphprs/ext-php-rs). It exposes Arrow's native readers and writers through
PHP streaming interfaces, letting PHP applications benefit from Rust-level performance without leaving the PHP runtime.

> [!TIP]
> The recommended way to use this extension is through the [parquet library](/documentation/components/libs/parquet.md), which provides a higher-level PHP API and automatically leverages the Arrow extension when it is loaded. You only need to use the classes documented here directly if you want low-level control over the Arrow reader/writer.

### Current Scope

The first module exposed through this extension is **Apache Parquet** - a columnar storage format widely used in data
engineering and analytics.

### Planned Modules

The Arrow Rust crates offer additional I/O capabilities that are candidates for future exposure through this extension:

- **CSV** - high-performance CSV reading/writing with automatic type inference and Arrow-native batching
- **JSON** - Arrow-backed JSON line (JSONL/NDJSON) reading/writing with schema support
- **IPC** - Arrow's own binary streaming/file format for zero-copy data exchange between processes and languages

## Features

- Read and write Apache Parquet files over `flow-php/filesystem` streams, value for value as `PhpParquetEngine`
- Nested types: LIST, STRUCT, MAP (arbitrarily nested)
- Compression codecs: UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4_RAW, BROTLI
- Column projection, offset and limit, read from one footer read
- Batches handed to other extensions through the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)

## Requirements

- PHP 8.3+
- Rust toolchain (rustc, cargo) - install from [rustup.rs](https://rustup.rs/)
- clang / libclang (for ext-php-rs bindgen)
- make

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/arrow-ext.md).

## Loading the Extension

### In php.ini

```ini
extension = arrow
```

### During Development

```bash
php -d extension=./ext/modules/arrow.so your_script.php
```

## Usage

`ArrowParquetEngine` of the [parquet library](/documentation/components/libs/parquet.md) is this extension:

```php
<?php

use Flow\Parquet\Reader;
use Flow\Parquet\Writer;

$reader = Reader::arrow();
$writer = Writer::arrow();
```

### Classes

```php
<?php

use Flow\Arrow\Parquet\{BatchReader, ColumnsReader, ParquetFile, RowsWriter};
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter};
use Flow\Parquet\Options;

use function Flow\Filesystem\DSL\{native_local_filesystem, path};

$filesystem = native_local_filesystem();

$file = new ParquetFile($filesystem->readFrom(path('data.parquet'))); // one footer read
$file->rowsNumber();
$file->thrift();                                                      // Flow\Parquet\ThriftModel\FileMetaData

$columns = new ColumnsReader($file, ['id', 'name'], batchSize: 1_000, offset: null, limit: null);

while (($chunk = $columns->next()) !== null) {
    // ['id' => [1, 2, ...], 'name' => ['a', 'b', ...]]
}

$writer = new RowsWriter(
    $filesystem->writeTo(path('out.parquet')),
    SchemaConverter::toExtension($schema),     // Flow\Parquet\ParquetFile\Schema
    'SNAPPY',
    OptionsConverter::toExtension(Options::default()),
    batchSize: 1_000,
);
$writer->writeRows([['id' => 1, 'name' => 'a']]);
$writer->writeColumns(['id' => [2, 3], 'name' => ['b', null]]);
$writer->close();                              // footer, then the stream closed

$batches = new BatchReader($file, ['id', 'name'], batchSize: 1_000, offset: null, limit: null);
$batches->schema();                            // Flow\Arrow\ArrowSchema
$batches->next();                              // Flow\Arrow\ArrowBatch, null after the last
```

Refusals are the `Flow\Parquet\Exception\*` the PHP engine throws; an exception the stream throws surfaces as itself.

### Arrow C Data Interface

`ArrowBatch` (`BatchReader::next()`) carries one struct array: `arrowSchemaAddress()` (`FFI_ArrowSchema*`) and
`arrowArrayAddress()` (`FFI_ArrowArray*`). Its consumer, another extension, reads the schema by reference and moves the
array out. `RowsWriter::writeBatch()` takes the same pair from any extension's final internal class exposing both
methods, its children by writer column name. Userland cannot build either: the classes are final and their
constructors throw.

### Interfaces

`Flow\Arrow\RandomAccessFile` (`read(int $length, int $offset): string`, `size(): ?int`) and
`Flow\Arrow\OutputStream` (`append(string $data): self`) are registered for stream implementations.

## Development

### Build Commands

```bash
make build    # Build the extension
make test     # Run PHPT tests
make install  # Install to system PHP
make clean    # Remove build artifacts
make rebuild  # Full clean + build
```

### Modifying the Extension

```bash
cd src/extension/arrow-ext
make rebuild
make test
```

## Architecture

- Built with [ext-php-rs](https://github.com/extphprs/ext-php-rs), which generates PHP bindings from Rust code
- Uses Apache Arrow and Parquet Rust crates from the [Arrow ecosystem](https://github.com/apache/arrow-rs)
- All compression codecs compiled into the extension - no external PHP compression extensions needed
- Stream methods (`read`, `size`, `append`, `close`) called from Rust via ext-php-rs
- PIE-compatible via `ext/config.m4` that delegates to `cargo build`

## See Also

- [parquet library](/documentation/components/libs/parquet.md)
- [ext-php-rs](https://github.com/extphprs/ext-php-rs)
- [Apache Arrow Rust](https://github.com/apache/arrow-rs)
- [Nix Development Environment](/documentation/contributing/nix.md)
