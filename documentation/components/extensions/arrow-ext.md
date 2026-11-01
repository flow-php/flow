---
package: flow-php/arrow-ext
---

# Arrow Extension

[PACKAGE_NAV]

[TOC]

[Apache Arrow](https://arrow.apache.org/) is a columnar memory format for flat and hierarchical data. It does not
depend on a programming language. The Arrow ecosystem has fast implementations of common data operations in C++, Rust,
Java, Python and other languages. These operations include I/O for formats such as Parquet, CSV, JSON and Arrow IPC.

This extension uses [ext-php-rs](https://github.com/extphprs/ext-php-rs) to bring the
[Arrow Rust ecosystem](https://github.com/apache/arrow-rs) into PHP. It gives PHP streaming interfaces to the native readers and
writers of Arrow. PHP applications then get the performance of Rust and stay in the PHP runtime.

> [!TIP]
> Use this extension through the [parquet library](/documentation/components/libs/parquet.md). The library has a higher-level PHP API. When the Arrow extension is loaded, the library uses it automatically. For low-level control over the Arrow reader/writer, use the classes on this page directly.

### Current Scope

The first module of this extension is Apache Parquet. Parquet is a columnar storage format that data engineering and
analytics use widely.

### Planned Modules

The Arrow Rust crates have more I/O features. This extension can add them in the future:

- CSV: fast CSV reading/writing with automatic type inference and Arrow-native batching
- JSON: JSON line (JSONL/NDJSON) reading/writing on Arrow, with schema support
- IPC: the binary streaming/file format of Arrow, for zero-copy data exchange between processes and languages

## Features

- Read and write Apache Parquet files over `flow-php/filesystem` streams, with the same values as `PhpParquetEngine`
- Nested types: LIST, STRUCT, MAP (arbitrarily nested)
- Compression codecs: UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4_RAW, BROTLI
- Column projection, offset and limit, from one read of the footer
- Batches that go to other extensions through the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)

## Requirements

- PHP 8.3+
- Rust toolchain (rustc, cargo), from [rustup.rs](https://rustup.rs/)
- clang / libclang (for ext-php-rs bindgen)
- make

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/arrow-ext.md).

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

The extension registers `Flow\Parquet\{ParquetEngine, ParquetFileReader, ParquetFileWriter}` of the
[parquet library](/documentation/components/libs/parquet.md). It implements them with
`Flow\Parquet\Engine\{RustParquetEngine, RustParquetFileReader, RustParquetFileWriter}`. `AdaptiveParquetEngine` is the
default of `Reader` and `Writer`. If the extension is loaded, it picks `RustParquetEngine`. If not, it picks
`PhpParquetEngine`.

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
use Flow\Parquet\ParquetFile\Index\StreamIndexes;
use Flow\Parquet\Reader\ReadPlan;

use function Flow\Filesystem\DSL\{native_local_filesystem, path};

$filesystem = native_local_filesystem();

$file = new RustParquetFileReader($filesystem->readFrom(path('data.parquet'))); // Flow\Parquet\ParquetFileReader, one footer read
$footer = $file->footer();                                            // Flow\Parquet\Engine\RustParquetFooter, readable after close()
$footer->rowsNumber();
$footer->schema();                                                    // Flow\Parquet\ParquetFile\Schema
$footer->metadata();                                                  // Flow\Parquet\ParquetFile\Metadata
$footer->thrift();                                                    // Flow\Parquet\ThriftModel\FileMetaData
$indexes = new StreamIndexes($footer, $filesystem->readFrom(path('data.parquet')));  // page indexes, read on demand
$plan = ReadPlan::window($footer->rowsNumber(), $indexes, offset: 10, limit: 100);  // rows 10-109

foreach ($file->readColumns(['id', 'name'], 1_000, $plan) as $chunk) { // RustColumnsReader, an Iterator that streams once
    // ['id' => [1, 2, ...], 'name' => ['a', 'b', ...]]
}

$writer = new RustParquetFileWriter(                      // Flow\Parquet\ParquetFileWriter
    $filesystem->writeTo(path('out.parquet')),
    SchemaConverter::toExtension($schema),     // Flow\Parquet\ParquetFile\Schema
    Compressions::SNAPPY,                      // LZO is refused
    OptionsConverter::toExtension(Options::default()),
    1_000,                                     // rows per written batch
);
$writer->writeBatch([['id' => 1, 'name' => 'a']]);   // an array or any Traversable of rows
$writer->writeColumns(['id' => [2, 3], 'name' => ['b', null]]);
$writer->close();                              // footer, then the stream closed

$batches = new RustBatchReader($file, ['id', 'name'], 1_000, $plan); // batch size, rows to read
$batches->schema();                            // Flow\Arrow\RustArrowSchema
$batches->next();                              // Flow\Arrow\RustParquetBatch, null after the last
```

Refusals are the same `Flow\Parquet\Exception\*` that the PHP engine throws. An exception from the stream comes out
unchanged.

### Arrow C Data Interface

`RustParquetBatch` (`RustBatchReader::next()`) carries one struct array: `arrowSchemaAddress()` (`FFI_ArrowSchema*`) and
`arrowArrayAddress()` (`FFI_ArrowArray*`). The consumer is another extension. It reads the schema by reference and
moves the array out. `RustParquetFileWriter::writeArrowBatch()` takes the same pair from a final internal class of
any extension that has both methods. The children of the array match the writer columns by name. Userland code cannot
make either class: the classes are final, and their constructors throw.

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
- All compression codecs are compiled into the extension, so no external PHP compression extensions are necessary
- Rust calls the stream methods (`read`, `size`, `append`, `close`) through ext-php-rs
- PIE-compatible through `ext/config.m4`, which calls `cargo build`

## See Also

- [parquet library](/documentation/components/libs/parquet.md)
- [ext-php-rs](https://github.com/extphprs/ext-php-rs)
- [Apache Arrow Rust](https://github.com/apache/arrow-rs)
- [Nix Development Environment](/documentation/contributing/nix.md)
