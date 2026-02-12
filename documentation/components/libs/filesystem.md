# Filesystem

- [⬅️️ Back](/documentation/introduction.md)
- [📦Packagist](https://packagist.org/packages/flow-php/filesystem)
- [🐙GitHub](https://github.com/flow-php/filesystem)
- [📚API Reference](/documentation/api/lib/filesystem)
- [📁Files](/documentation/api/lib/filesystem/indices/files.html)
- [🗺DSL](/documentation/api/lib/filesystem/namespaces/flow-filesystem-dsl.html)

[TOC]

## Installation

```
composer require flow-php/filesystem:~--FLOW_PHP_VERSION--
```

Flow Filesystem is a unified solution to store and retrieve data at remote and local filesystems.
What differentiates Flow Filesystem from other libraries is the ability to store data in Blocks and read
it by byte ranges.

This means, that while writing data to a large remote file, instead we can literally stream the data and based on the
implementation
of the filesystem, it will be saved in blocks.

When reading, instead of iterating through the whole file to find the data you need, you can directly access the data
you need by specifying the byte range.

# Available Filesystems

- [Native Local Filesystem](/src/lib/filesystem/src/Flow/Filesystem/Local/NativeLocalFilesystem.php)
- [Memory Filesystem](/src/lib/filesystem/src/Flow/Filesystem/Local/MemoryFilesystem.php)
- [StdOut Filesystem](/src/lib/filesystem/src/Flow/Filesystem/Local/StdOutFilesystem.php)
- [Azure Blob Filesystem](/documentation/components/bridges/filesystem-azure-bridge) - [
  `flow-php/filesystem-azure-bridge`](https://packagist.org/packages/flow-php/filesystem-azure-bridge)
- [AWS S3 Filesystem](/documentation/components/bridges/filesystem-async-aws-bridge) - [
  `flow-php/filesystem-async-aws-bridge`](https://packagist.org/packages/flow-php/filesystem-async-aws-bridge)

# Building Blocks

- `SourceStream` - source streams interface represents readonly data streams

```php
<?php

SourceStream::content() : string;
SourceStream::iterate(int $length = 1) : \Generator;
SourceStream::read(int $length, int $offset) : string;
SourceStream::readLines(string $separator = "\n", ?int $length = null) : \Generator;
SourceStream::size() : ?int;
```

- `DestinationStream` - destination streams interface represents writable data streams

```php
DestinationStream::append(string $data) : self;
DestinationStream::fromResource($resource) : self;
```

- `Filesystem` - filesystem interface represents a remote/local filesystem

```php
<?php

Filesystem::list(Path $path, Filter $pathFilter = new KeepAll()) : \Generator;
Filesystem::mv(Path $from, Path $to) : bool;
Filesystem::protocol() : Protocol;
Filesystem::readFrom(Path $path) : SourceStream;
Filesystem::rm(Path $path) : bool;
Filesystem::status(Path $path) : ?FileStatus;
Filesystem::writeTo(Path $path) : DestinationStream;
```

- `FilesystemTable` - a registry for all filesystems

```php
<?php

FilesystemTable::for(Path|Protocol $path) : Filesystem
FilesystemTable::mount(Filesystem $filesystem) : void
FilesystemTable::unmount(Filesystem $filesystem) : void
```

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
use function Flow\Filesystem\DSL\protocol;

$fstab = fstab(
    azure_filesystem(
        azure_blob_service(
            azure_blob_service_config($account, $container),
            azure_shared_key_authorization_factory($account, $accountKey),
        ),
        azure_filesystem_options()
    )
);


$stream = $fstab->for(protocol('azure-blob'))->writeTo(path('azure-blob://orders.csv'));

$stream->append('id,name,active');
$stream->append('1,norbert,true');
$stream->append('2,john,true');
$stream->append('3,jane,true');
$stream->close();
```

## Telemetry

Flow Filesystem supports OpenTelemetry-compatible tracing and metrics for observability of all filesystem operations.
Flow Filesystem uses [Flow Telemetry](/documentation/components/libs/telemetry) library.

In order to use telemetry, you need to create an instance of `TraceableFilesystem` which 
wraps an existing filesystem and adds telemetry to it.

Alternatively you can pass `FilesystemTelemetryConfig` to `FilesystemTable` and let it 
automatically wrap all mounted filesystems with telemetry.

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

**Spans:**

- `SourceStream` - spans the lifecycle of a read stream from creation to close
- `DestinationStream` - spans the lifecycle of a write stream from creation to close

**Metrics:**

- `filesystem.source.bytes_read` - total bytes read from source streams
- `filesystem.source.operations` - number of read operations
- `filesystem.destination.bytes_written` - total bytes written to destination streams
- `filesystem.destination.operations` - number of write operations

Metadata operations (list, status, rm, mv) are logged but do not create spans.

### Examples

**Wrap an individual filesystem:**

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

**Enable telemetry on FilesystemTable:**

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

**Disable specific features:**

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