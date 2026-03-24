# Arrow PHP Extension

A Rust-powered PHP extension for reading and writing Apache Parquet files using [ext-php-rs](https://github.com/davidcole1340/ext-php-rs) and the official Apache Arrow/Parquet Rust crates.

## Features

- Read and write Parquet files through streaming interfaces (`RandomAccessFile`, `OutputStream`)
- All flat Arrow types: INT32, INT64, FLOAT, DOUBLE, BOOLEAN, STRING, BINARY, DATE32, TIMESTAMP
- Nested types: LIST, STRUCT, MAP
- Compression: UNCOMPRESSED, SNAPPY, GZIP, BROTLI, ZSTD, LZ4_RAW
- Column projection for selective reads
- Configurable row group size, compression level, and writer version

## Installation

### Using PIE (Recommended)

[PIE](https://github.com/php/pie) is the modern PHP extension installer.

**Prerequisites:** Install Rust toolchain and clang on your system:

```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Ubuntu/Debian
sudo apt-get install build-essential clang libclang-dev

# macOS with Homebrew
brew install llvm
export LIBCLANG_PATH=$(brew --prefix llvm)/lib
```

**Install the extension:**

```bash
pie install flow-php/arrow-ext
```

### Requirements

- PHP 8.3+
- Rust toolchain (rustc, cargo) — install from https://rustup.rs/
- clang/libclang (for ext-php-rs bindgen)
- make

### Manual Build

```bash
cd src/extension/arrow-ext

# Install build dependencies (Ubuntu/Debian)
sudo apt-get install build-essential clang libclang-dev

# Install build dependencies (macOS)
brew install llvm
export LIBCLANG_PATH=$(brew --prefix llvm)/lib

# Build the extension
make build

# Run tests
make test

# Install to system PHP (optional)
make install
```

### Using Nix

From the Flow PHP monorepo root:

```bash
# Default shell includes the pre-built arrow extension
nix-shell
php -m | grep arrow

# For extension development (Rust toolchain + PHP dev headers, no pre-built extension)
nix-shell --arg with-arrow-ext false --arg with-rust true
cd src/extension/arrow-ext
make build && make test
```

## Usage

### Reading Parquet Files

```php
use Flow\Arrow\Parquet\Reader;
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

$reader = new Reader(new FileSource('data.parquet'));

$schema = $reader->schema();
$metadata = $reader->metadata();

while ($batch = $reader->readRowGroup(['id', 'name'])) {
    foreach ($batch['id'] as $i => $id) {
        echo "$id: {$batch['name'][$i]}\n";
    }
}

$reader->close();
```

### Writing Parquet Files

```php
use Flow\Arrow\Parquet\Writer;
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

## Loading the Extension

### During Development

```bash
php -d extension=./ext/modules/arrow.so your_script.php
```

### In php.ini

```ini
extension=arrow
```

## API Reference

### Interfaces

| Interface | Method | Parameters | Returns |
|---|---|---|---|
| `RandomAccessFile` | `read` | `int $length, int $offset` | `string` |
| `RandomAccessFile` | `size` | — | `?int` |
| `OutputStream` | `append` | `string $data` | `self` |

### Classes

| Class | Method | Parameters | Returns |
|---|---|---|---|
| `Reader` | `__construct` | `RandomAccessFile $source, array $options = []` | — |
| `Reader` | `schema` | — | `array` |
| `Reader` | `metadata` | — | `array` |
| `Reader` | `readRowGroup` | `?array $columns = null` | `?array` |
| `Reader` | `close` | — | `void` |
| `Writer` | `__construct` | `OutputStream $stream, array $schema, string $compression = 'SNAPPY', array $options = []` | — |
| `Writer` | `writeBatch` | `array $batch` | `void` |
| `Writer` | `close` | — | `void` |
| `Exception` | — | extends `\RuntimeException` | — |

## Development

```bash
make build    # Build the extension
make test     # Run PHPT tests
make install  # Install to system PHP
make clean    # Remove build artifacts
make rebuild  # Full clean + build
```

## License

MIT
