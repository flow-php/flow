---
package: flow-php/filesystem
---

# Filesystem

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/filesystem.md).

Flow Filesystem is one API to store and get data on remote and local filesystems.
Unlike other libraries, Flow Filesystem can store data in Blocks and read it by byte ranges.

When you write data to a large remote file, you can stream the data. The filesystem implementation then saves it in
blocks.

When you read, you do not have to go through the full file to find the data. You give the byte range, and you get
that data directly.

# Available Filesystems

- [Native Local Filesystem](/src/lib/filesystem/src/Flow/Filesystem/Local/NativeLocalFilesystem.php)
- [Memory Filesystem](/src/lib/filesystem/src/Flow/Filesystem/Local/MemoryFilesystem.php)
- [StdOut Filesystem](/src/lib/filesystem/src/Flow/Filesystem/Local/StdOutFilesystem.php)
- [Azure Blob Filesystem](/documentation/components/bridges/filesystem-azure-bridge) - [
  `flow-php/filesystem-azure-bridge`](https://packagist.org/packages/flow-php/filesystem-azure-bridge)
- [AWS S3 Filesystem](/documentation/components/bridges/filesystem-async-aws-bridge) - [
  `flow-php/filesystem-async-aws-bridge`](https://packagist.org/packages/flow-php/filesystem-async-aws-bridge)
- [SFTP Filesystem](/documentation/components/bridges/filesystem-sftp-bridge) - [
  `flow-php/filesystem-sftp-bridge`](https://packagist.org/packages/flow-php/filesystem-sftp-bridge)

# Building Blocks

- `SourceStream` - source streams interface represents readonly data streams

```php ignore
<?php

SourceStream::content() : string;
SourceStream::iterate(int $length = 1) : \Generator;
SourceStream::read(int $length, int $offset) : string;
SourceStream::readLines(string $separator = "\n", ?int $length = null) : \Generator;
SourceStream::size() : ?int;
```

- `DestinationStream` - destination streams interface represents writable data streams

```php ignore
DestinationStream::append(string $data) : self;
DestinationStream::fromResource($resource) : self;
```

- `Mount` - a value object that identifies a filesystem mount by its URI protocol (`file`, `memory`, `aws-s3`, `warehouse`, ...)

```php
<?php

final readonly class Mount
{
    public string $protocol;

    public function __construct(string $protocol); // validates the protocol against PROTOCOL_REGEX
    public function supports(Path|string $path) : bool; // true when $path's URI protocol matches
}
```

- `Filesystem` - filesystem interface represents a remote/local filesystem

```php ignore
<?php

Filesystem::appendTo(Path $path) : DestinationStream;
Filesystem::create(Path $path) : DestinationStream; // visible whole at close(), FileExistsException when the path exists
Filesystem::getSystemTmpDir() : Path;
Filesystem::list(Path $path, Filter $pathFilter = new KeepAll()) : \Generator; // yields FileStatus
Filesystem::mount() : Mount;
Filesystem::mv(Path $from, Path $to) : bool;
Filesystem::readFrom(Path $path) : SourceStream;
Filesystem::rm(Path $path, bool $recursive = true) : bool; // $recursive = false refuses a non-empty directory
Filesystem::status(Path $path) : ?FileStatus;
Filesystem::writeTo(Path $path) : DestinationStream;
```

- `FileStatus` - metadata returned from `status()` / `list()`

```php
<?php

final readonly class FileStatus
{
    public Path $path;
    public ?int $size;                        // bytes, null for directories
    public ?\DateTimeImmutable $lastModifiedAt; // populated when the backend exposes it

    public function isFile() : bool;
    public function isDirectory() : bool;
}
```

`size` and `lastModifiedAt` come from the list/head response of the backend, at no cost. No extra stream opens, and
no extra HTTP call runs. If the backend cannot give them, they are `null`, for example in `StdOutFilesystem`.

- `Path::protocol() : string` - returns the raw URI scheme (`'file'`, `'aws-s3'`, `'warehouse'`).

- `FilesystemTable` - a registry of filesystems, with the mount protocol as the key

```php ignore
<?php

FilesystemTable::for(Path|string $protocol) : Filesystem;
FilesystemTable::mount(Filesystem $filesystem) : void;
FilesystemTable::unmount(Filesystem $filesystem) : void;
```

Each mount must have a unique protocol. Two mounts with the same protocol throw
`InvalidArgumentException`. You can mount a filesystem under any protocol, for example two S3 buckets under
`warehouse` and `archive`. At runtime, you get them with `$table->for('warehouse')` or `$table->for($path)`.

## Usage

```php
<?php

use function Flow\Azure\SDK\DSL\azure_blob_service;
use function Flow\Azure\SDK\DSL\azure_blob_service_config;
use function Flow\Azure\SDK\DSL\azure_shared_key_authorization_factory;
use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem_options;
use function Flow\Filesystem\DSL\fstab;
use function Flow\Filesystem\DSL\path;

$fstab = fstab(
    azure_filesystem(
        azure_blob_service(
            azure_blob_service_config($account, $container),
            azure_shared_key_authorization_factory($account, $accountKey),
        ),
        azure_filesystem_options()
    )
);

$stream = $fstab->for('azure-blob')->writeTo(path('azure-blob://orders.csv'));

$stream->append('id,name,active');
$stream->append('1,norbert,true');
$stream->append('2,john,true');
$stream->append('3,jane,true');
$stream->close();
```

## Glob patterns

All filesystems (local, memory, S3, Azure) list the same files for the same pattern.

| token                   | matches                                                                    |
|-------------------------|----------------------------------------------------------------------------|
| `*`                     | any run of characters inside one path segment, a leading `.` included      |
| `?`                     | one character inside one segment                                           |
| `[abc]` `[a-z]`         | one character of the set / range, never `/`                                |
| `[!abc]`                | one character outside the set, never `/`, and `^` inside `[]` is a literal |
| `**` as a whole segment | zero or more segments, and as the last segment: everything below           |
| `**` inside a segment   | same as `*`                                                                |
| `{name}`                | flow partition placeholder: one or more characters inside one segment      |
| unclosed `[`            | the literal `[`. A `]` after a `/` does not close it                       |

```php
<?php

use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;

// data/flat.parquet, data/date=2026-09-01/one.parquet, data/id=1/date=2026-09-01/two.parquet
native_local_filesystem()->list(path(__DIR__ . '/data/**/*.parquet')); // all three
native_local_filesystem()->list(path(__DIR__ . '/data/**.parquet'));   // data/flat.parquet
```

A local listing follows the symlinks that a pattern names. `**` never goes into a symlinked directory. There is no
escape character.

## Exclusive create

`create()` writes a file that becomes visible as a whole at `close()`. If the path exists at that time, `close()`
throws `FileExistsException`, and the existing file does not change. Local, memory, S3 and Azure filesystems support
it. SFTP and StdOut throw.

```php
<?php

use Flow\Filesystem\Exception\FileExistsException;

use function Flow\Filesystem\DSL\{native_local_filesystem, path};

try {
    native_local_filesystem()->create(path(__DIR__ . '/claims/42'))->append((string) getmypid())->close();
} catch (FileExistsException) {
    // another process created it first
}
```

## Local permissions

`NativeLocalFilesystem` applies `Permissions` to each file that it writes and each directory that it creates. It does
not change existing directories. Without `Permissions`, the umask decides.

```php
<?php

use Flow\Filesystem\Local\Permissions;

use function Flow\Filesystem\DSL\{native_local_filesystem, path};

$filesystem = native_local_filesystem(permissions: Permissions::private()); // files 0600, directories 0700

$filesystem->writeTo(path(__DIR__ . '/secrets/token.txt'))->append($token)->close();
```

## Cross-filesystem copy & move

With `FilesystemTable` and the `Copy` / `Move` operations, you can copy or move files between two mounted
filesystems. A move in the same filesystem uses the native `mv` of the backend (local rename, S3
CopyObject + DeleteObject, Azure CopyBlob + DeleteBlob). A move across filesystems streams bytes from the source to
the destination in chunks, and then removes the source. This move is not atomic. If the removal of the source fails
after a successful write, the destination is present and the source stays. A second run is idempotent (safe to run
again).

```php
<?php

use function Flow\Filesystem\DSL\{file_copy, file_move, fstab, memory_filesystem, native_local_filesystem, operation_options, path};

$table = fstab(memory_filesystem(), native_local_filesystem());

// Seed a memory file
$table->for('memory')->writeTo(path('memory://hello.txt'))->append('hello')->close();

// Cross-filesystem copy: memory → local
file_copy($table)->execute(path('memory://hello.txt'), path('/tmp/hello.txt'));

// Cross-filesystem move with custom chunk size
file_move($table, operation_options(chunkSize: 64 * 1024))
    ->execute(path('memory://hello.txt'), path('/tmp/moved.txt'));
```

`operation_options()` accepts one `chunkSize` (default `8192`). It sets the byte-chunk size for streaming copies
across filesystems.

## Size formatting

`Flow\Filesystem\SizeUnits::humanReadable()` formats byte counts with binary units
(B, KiB, MiB, GiB, TiB, PiB). For the fractional part, it has the same signature as `number_format` of PHP:

```php
<?php

use Flow\Filesystem\SizeUnits;

SizeUnits::humanReadable(0);          // "0 B"
SizeUnits::humanReadable(1023);       // "1,023 B"
SizeUnits::humanReadable(1536);       // "1.50 KiB"
SizeUnits::humanReadable(1_048_576);  // "1.00 MiB"
SizeUnits::humanReadable(null);       // "-"

// Custom formatting
SizeUnits::humanReadable(1536, decimals: 0);                  // "2 KiB"
SizeUnits::humanReadable(1536, decimalSeparator: ',');        // "1,50 KiB"
SizeUnits::humanReadable(null, null: 'n/a');                  // "n/a"
```

## Telemetry

Flow Filesystem supports OpenTelemetry-compatible tracing and metrics for all filesystem operations.
Flow Filesystem uses the [Flow Telemetry](/documentation/components/libs/telemetry) library.

To use telemetry, create an instance of `TraceableFilesystem`. It
wraps an existing filesystem and adds telemetry to it.

You can also give `FilesystemTelemetryConfig` to `FilesystemTable`. It then
wraps all mounted filesystems with telemetry.

### DSL Functions

- `filesystem_telemetry_options()` - configure what to trace and measure
- `filesystem_telemetry_config()` - create telemetry configuration from options
- `traceable_filesystem()` - wrap an individual filesystem with telemetry

### Configuration Options

| Option           | Default | Description                                       |
|------------------|---------|---------------------------------------------------|
| `traceStreams`   | `true`  | Create spans for stream lifecycle (open to close) |
| `collectMetrics` | `true`  | Collect bytes and operation counters              |

### What Gets Traced

Spans:

- `filesystem.read` - spans the lifecycle of a read stream from creation to close
- `filesystem.write` - spans the lifecycle of a write stream from creation to close

Spans carry `flow.filesystem.path.uri`, `flow.filesystem.protocol`, `flow.filesystem.stream.type` and, on close,
`flow.filesystem.bytes.total_read` / `flow.filesystem.bytes.total_written`.

Metrics:

- `flow.filesystem.read.size` (`By`) - total bytes read from source streams
- `flow.filesystem.read.operations` (`{operation}`) - number of read operations
- `flow.filesystem.write.size` (`By`) - total bytes written to destination streams
- `flow.filesystem.write.operations` (`{operation}`) - number of write operations

Metadata operations (list, status, rm, mv) are logged but do not create spans.

### Examples

Wrap one filesystem:

```php
<?php

use function Flow\Filesystem\DSL\{
    filesystem_telemetry_config,
    filesystem_telemetry_options,
    native_local_filesystem,
    path,
    traceable_filesystem
};
use function Flow\Telemetry\DSL\telemetry;
use Psr\Clock\ClockInterface;

$telemetry = telemetry(/* your configuration */);
$clock = new class implements ClockInterface {
    public function now(): \DateTimeImmutable {
        return new \DateTimeImmutable();
    }
};

$config = filesystem_telemetry_config(
    $telemetry,
    $clock,
    filesystem_telemetry_options(traceStreams: true, collectMetrics: true)
);

$fs = traceable_filesystem(native_local_filesystem(), $config);

// All operations on $fs are now traced
$stream = $fs->readFrom(path('/path/to/file.csv'));
```

Enable telemetry on FilesystemTable:

```php
<?php

use function Flow\Filesystem\DSL\{
    filesystem_telemetry_config,
    filesystem_telemetry_options,
    fstab
};

$config = filesystem_telemetry_config($telemetry, $clock);
$fstab = fstab();
$fstab->withTelemetry($config);

// All filesystems in the table are now wrapped with telemetry
```

Disable specific features:

```php
<?php

use function Flow\Filesystem\DSL\filesystem_telemetry_options;

// Collect metrics only, no spans
$options = filesystem_telemetry_options(
    traceStreams: false,
    collectMetrics: true
);

// Trace streams only, no metrics
$options = filesystem_telemetry_options(
    traceStreams: true,
    collectMetrics: false
);
```
