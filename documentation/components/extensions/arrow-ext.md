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

The extension registers the [parquet library](/documentation/components/libs/parquet.md)'s
`Flow\Parquet\{ParquetEngine, ParquetFileReader, ParquetFileWriter}` and implements them with
`Flow\Parquet\Engine\{RustParquetEngine, RustParquetFileReader, RustParquetFileWriter}`. `AdaptiveParquetEngine` - the
default of `Reader` and `Writer` - picks `RustParquetEngine` while the extension is loaded, `PhpParquetEngine`
otherwise.

```php
<?php

use Flow\Parquet\{Reader, Writer};

$reader = new Reader(); // Flow\Parquet\Engine\AdaptiveParquetEngine
$writer = new Writer();
```

### Classes

```php
<?php

use Flow\Arrow\Parquet\RustBatchReader;
use Flow\Parquet\Engine\{RustParquetFileReader, RustParquetFileWriter};
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter};
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;

use function Flow\Filesystem\DSL\{native_local_filesystem, path};

$filesystem = native_local_filesystem();

$file = new RustParquetFileReader($filesystem->readFrom(path('data.parquet'))); // Flow\Parquet\ParquetFileReader, one footer read
$file->rowsNumber();
$file->schema();                                                      // Flow\Parquet\ParquetFile\Schema
$file->metadata();                                                    // Flow\Parquet\ParquetFile\Metadata
$file->thrift();                                                      // Flow\Parquet\ThriftModel\FileMetaData

foreach ($file->readColumns(['id', 'name'], 1_000, null, null) as $chunk) { // RustColumnsReader, an Iterator that streams once
    // ['id' => [1, 2, ...], 'name' => ['a', 'b', ...]]
}

$writer = new RustParquetFileWriter(                      // Flow\Parquet\ParquetFileWriter
    $filesystem->writeTo(path('out.parquet')),
    SchemaConverter::toExtension($schema),     // Flow\Parquet\ParquetFile\Schema
    Compressions::SNAPPY,                      // LZO is refused
    OptionsConverter::toExtension(Options::default()),
    batchSize: 1_000,
);
$writer->writeBatch([['id' => 1, 'name' => 'a']]);   // an array or any Traversable of rows
$writer->writeColumns(['id' => [2, 3], 'name' => ['b', null]]);
$writer->close();                              // footer, then the stream closed

$batches = new RustBatchReader($file, ['id', 'name'], batchSize: 1_000, offset: null, limit: null);
$batches->schema();                            // Flow\Arrow\RustArrowSchema
$batches->next();                              // Flow\Arrow\RustParquetBatch, null after the last
```

Refusals are the `Flow\Parquet\Exception\*` the PHP engine throws; an exception the stream throws surfaces as itself.

### Arrow C Data Interface

`RustParquetBatch` (`RustBatchReader::next()`) carries one struct array: `arrowSchemaAddress()` (`FFI_ArrowSchema*`) and
`arrowArrayAddress()` (`FFI_ArrowArray*`). Its consumer, another extension, reads the schema by reference and moves the
array out. `RustParquetFileWriter::writeArrowBatch()` takes the same pair from any extension's final internal class exposing both
methods, its children by writer column name. Userland cannot build either: the classes are final and their
constructors throw.

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
