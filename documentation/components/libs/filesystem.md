# Filesystem

- [⬅️️ Back](../../introduction.md)

## Installation

```
composer require flow-php/filesystem
```

Flow Filesystem is a unified solution to store and retrieve data at remote and local filesystems. 
What differentiates Flow Filesystem from other libraries is the ability to store data in Blocks and read 
it by byte ranges. 

This means, that while writing data to a large remote file, instead we can literally stream the data and based on the implementation
of the filesystem, it will be saved in blocks. 

When reading, instead of iterating through the whole file to find the data you need, you can directly access the data you need by specifying the byte range.

# Available Filesystems

- Native Local Filesystem 
- [Azure Blob Filesystem](https://github.com/flow-php/flow/blob/1.x/documentation/components/bridges/filesystem-azure-bridge.md)

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