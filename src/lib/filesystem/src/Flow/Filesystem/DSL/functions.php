<?php

declare(strict_types=1);

namespace Flow\Filesystem\DSL;

use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\{Filesystem, FilesystemTable, Partition, Partitions, Path};

function partition(string $name, string $value) : Partition
{
    return new Partition($name, $value);
}

function partitions(Partition ...$partition) : Partitions
{
    return new Partitions(...$partition);
}

/**
 * @param array<string, mixed> $options
 */
function path(string $path, array $options = []) : Path
{
    return new Path($path, $options);
}

function path_real(string $path, array $options = []) : Path
{
    return Path::realpath($path, $options);
}

function native_local_filesystem() : NativeLocalFilesystem
{
    return new NativeLocalFilesystem();
}

function fstab(Filesystem ...$filesystems) : FilesystemTable
{
    if (!\count($filesystems)) {
        $filesystems[] = native_local_filesystem();
    }

    return new FilesystemTable(...$filesystems);
}
