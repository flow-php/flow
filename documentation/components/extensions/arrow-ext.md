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

- Read and write Apache Parquet files through PHP streaming interfaces
- Flat types: INT32, INT64, FLOAT, DOUBLE, BOOLEAN, STRING, BINARY, DATE32, TIMESTAMP
- Nested types: LIST, STRUCT, MAP (arbitrarily nested)
- Compression codecs: UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4_RAW, BROTLI
- Column projection for selective reads
- Configurable row group size, compression level, and writer version
- Columnar batch I/O for maximum throughput

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

### Implementing the Streaming Interfaces

The extension operates on two PHP interfaces for I/O. You must provide implementations for your storage backend.

**Source (reading):**

```php
<?php

use Flow\Arrow\RandomAccessFile;

class FileSource implements RandomAccessFile
{
    private readonly string $data;

    public function __construct(string $path)
    {
        $this->data = file_get_contents($path);
    }

    public function read(int $length, int $offset): string
    {
        return substr($this->data, $offset, $length);
    }

    public function size(): ?int
    {
        return strlen($this->data);
    }
}
```

**Destination (writing):**

```php
<?php

use Flow\Arrow\OutputStream;

class FileDestination implements OutputStream
{
    /** @var resource */
    private $fh;

    public function __construct(string $path)
    {
        $this->fh = fopen($path, 'wb');
    }

    public function append(string $data): self
    {
        fwrite($this->fh, $data);
        return $this;
    }

    public function __destruct()
    {
        fclose($this->fh);
    }
}
```

### Reading Parquet Files

```php
<?php

use Flow\Arrow\Parquet\Reader;

$reader = new Reader(new FileSource('data.parquet'));

// Get schema and metadata
$schema = $reader->schema();
$metadata = $reader->metadata();

// Read row groups (with optional column projection)
while ($batch = $reader->readRowGroup(['id', 'name'])) {
    // $batch is ['column_name' => [values...], ...]
    foreach ($batch['id'] as $i => $id) {
        echo "$id: {$batch['name'][$i]}\n";
    }
}

$reader->close();
```

### Writing Parquet Files

```php
<?php

use Flow\Arrow\Parquet\Writer;

$schema = [
    ['name' => 'id', 'type' => 'INT64', 'optional' => false],
    ['name' => 'name', 'type' => 'STRING', 'optional' => true],
];

$writer = new Writer(new FileDestination('output.parquet'), $schema, 'SNAPPY');
$writer->writeBatch([
    'id' => [1, 2, 3],
    'name' => ['Alice', 'Bob', null],
]);
$writer->close();
```

### Schema Definition

The schema is an array of column definitions. Each column has a `name`, `type`, and optional `optional` flag.

| Type | PHP Read Value | Notes |
|------|---------------|-------|
| `BOOLEAN` | `bool` | |
| `INT32` | `int` | |
| `INT64` | `int` | |
| `FLOAT` | `float` | |
| `DOUBLE` | `float` | |
| `STRING` | `string` | |
| `BINARY` | `string` (raw bytes) | |
| `DATE32` | `string` (YYYY-MM-DD) | |
| `TIMESTAMP` | `string` (ISO 8601) | |
| `LIST` | `array` | Requires `children` key with 1 element |
| `STRUCT` | `array` (associative) | Requires `children` key with N elements |
| `MAP` | `array` (associative) | Requires `children` key with 2 elements (key + value) |

**Nested schema example:**

```php
<?php

$schema = [
    ['name' => 'id', 'type' => 'INT64', 'optional' => false],
    ['name' => 'tags', 'type' => 'LIST', 'optional' => true, 'children' => [
        ['name' => 'element', 'type' => 'STRING', 'optional' => true],
    ]],
    ['name' => 'address', 'type' => 'STRUCT', 'optional' => true, 'children' => [
        ['name' => 'street', 'type' => 'STRING', 'optional' => true],
        ['name' => 'city', 'type' => 'STRING', 'optional' => true],
    ]],
    ['name' => 'metadata', 'type' => 'MAP', 'optional' => true, 'children' => [
        ['name' => 'key', 'type' => 'STRING', 'optional' => false],
        ['name' => 'value', 'type' => 'STRING', 'optional' => true],
    ]],
];
```

### Writer Options

Options are passed as the fourth argument to the `Writer` constructor:

```php
<?php

$writer = new Writer($stream, $schema, 'SNAPPY', [
    'ROW_GROUP_SIZE_BYTES' => 128 * 1024 * 1024,
    'WRITER_VERSION' => '2.0',
]);
```

| Option Key | Type | Description |
|---|---|---|
| `ROW_GROUP_SIZE_BYTES` | `int` | Maximum row group size in bytes |
| `COMPRESSION_LEVEL` | `int` | Compression level (codec-specific) |
| `WRITER_VERSION` | `string` | `"1.0"` or `"2.0"` |

## API Reference

### Interfaces

**`Flow\Arrow\RandomAccessFile`**

| Method | Parameters | Returns | Description |
|---|---|---|---|
| `read` | `int $length, int $offset` | `string` | Read `$length` bytes starting at `$offset` |
| `size` | - | `?int` | Return total size in bytes, or `null` if unknown |

**`Flow\Arrow\OutputStream`**

| Method | Parameters | Returns | Description |
|---|---|---|---|
| `append` | `string $data` | `self` | Append data to the output stream |

### Classes

**`Flow\Arrow\Parquet\Reader`**

| Method | Parameters | Returns | Description |
|---|---|---|---|
| `__construct` | `RandomAccessFile $source, array $options = []` | - | Open a Parquet source for reading |
| `schema` | - | `array` | Return the file schema as nested arrays |
| `metadata` | - | `array` | Return file-level metadata (row count, row groups, key-value metadata) |
| `readRowGroup` | `?array $columns = null` | `?array` | Read next row group as columnar batch, or `null` when exhausted |
| `close` | - | `void` | Release resources |

**`Flow\Arrow\Parquet\Writer`**

| Method | Parameters | Returns | Description |
|---|---|---|---|
| `__construct` | `OutputStream $stream, array $schema, string $compression = 'SNAPPY', array $options = []` | - | Open a Parquet destination for writing |
| `writeBatch` | `array $batch` | `void` | Write a columnar batch (`['col' => [values...]]`) |
| `close` | - | `void` | Flush and finalize the Parquet file |

**`Flow\Arrow\Parquet\Exception`**

Extends `\RuntimeException`. Thrown on all Parquet read/write errors originating from the Rust layer.

## Error Handling

```php
<?php

use Flow\Arrow\Parquet\Exception;
use Flow\Arrow\Parquet\Reader;

try {
    $reader = new Reader(new FileSource('data.parquet'));
    while ($batch = $reader->readRowGroup()) {
        // process batch
    }
    $reader->close();
} catch (Exception $e) {
    echo "Parquet error: " . $e->getMessage();
}
```

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
- PHP streaming interfaces (`RandomAccessFile`, `OutputStream`) called from Rust via ext-php-rs callbacks
- Columnar batch format aligns with Parquet's native columnar storage
- PIE-compatible via `ext/config.m4` that delegates to `cargo build`

## See Also

- [parquet library](/documentation/components/libs/parquet.md)
- [ext-php-rs](https://github.com/extphprs/ext-php-rs)
- [Apache Arrow Rust](https://github.com/apache/arrow-rs)
- [Nix Development Environment](/documentation/contributing/nix.md)
