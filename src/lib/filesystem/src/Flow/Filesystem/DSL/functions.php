<?php

declare(strict_types=1);

namespace Flow\Filesystem\DSL;

use Flow\ETL\Attribute\{DocumentationDSL, Module, Type};
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\{Filesystem, FilesystemTable, Partition, Partitions, Path, Protocol};

#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function protocol(string $protocol) : Protocol
{
    return new Protocol($protocol);
}

#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function partition(string $name, string $value) : Partition
{
    return new Partition($name, $value);
}

#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function partitions(Partition ...$partition) : Partitions
{
    return new Partitions(...$partition);
}

/**
 * Path supports glob patterns.
 * Examples:
 *  - path('*.csv') - any csv file in current directory
 *  - path('/** / *.csv') - any csv file in any subdirectory (remove empty spaces)
 *  - path('/dir/partition=* /*.parquet') - any parquet file in given partition directory.
 *
 * Glob pattern is also supported by remote filesystems like Azure
 *
 *  - path('azure-blob://directory/*.csv') - any csv file in given directory
 *
 * @param array<string, mixed> $options
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function path(string $path, array $options = []) : Path
{
    return new Path($path, $options);
}

/**
 * Resolve real path from given path.
 *
 * @param array<string, mixed> $options
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function path_real(string $path, array $options = []) : Path
{
    return Path::realpath($path, $options);
}

#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function native_local_filesystem() : NativeLocalFilesystem
{
    return new NativeLocalFilesystem();
}

/**
 * Create a new filesystem table with given filesystems.
 * Filesystems can be also mounted later.
 * If no filesystems are provided, local filesystem is mounted.
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function fstab(Filesystem ...$filesystems) : FilesystemTable
{
    if (!\count($filesystems)) {
        $filesystems[] = native_local_filesystem();
    }

    return new FilesystemTable(...$filesystems);
}
