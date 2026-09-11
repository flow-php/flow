<?php

declare(strict_types=1);

namespace Flow\Filesystem\DSL;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Local\MemoryFilesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Local\StdOutFilesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Operations\Copy;
use Flow\Filesystem\Operations\Move;
use Flow\Filesystem\Operations\OperationOptions;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Options;
use Flow\Filesystem\Telemetry\FilesystemTelemetryConfig;
use Flow\Filesystem\Telemetry\FilesystemTelemetryOptions;
use Flow\Filesystem\Telemetry\TraceableFilesystem;
use Flow\Telemetry\Telemetry;
use Psr\Clock\ClockInterface;

use function count;

#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function mount(string $protocol): Mount
{
    return new Mount($protocol);
}

#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function partition(string $name, string $value): Partition
{
    return new Partition($name, $value);
}

#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function partitions(Partition ...$partition): Partitions
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
 * @param array<string, null|bool|float|int|string|\UnitEnum>|Path\Options $options
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function path(string $path, array|Options $options = []): Path
{
    return Path::from($path, $options);
}

/**
 * Resolve real path from given path.
 *
 * @param array<string, null|bool|float|int|string|\UnitEnum> $options
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function path_real(string $path, array $options = []): Path
{
    return Path::realpath($path, $options);
}

#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function native_local_filesystem(string $protocol = 'file'): NativeLocalFilesystem
{
    return new NativeLocalFilesystem(new Mount($protocol));
}

/**
 * Write-only filesystem useful when we just want to write the output to stdout.
 * The main use case is for streaming datasets over http.
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function stdout_filesystem(string $protocol = 'stdout'): StdOutFilesystem
{
    return new StdOutFilesystem(new Mount($protocol));
}

/**
 * Create a new memory filesystem and writes data to it in memory.
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function memory_filesystem(string $protocol = 'memory'): MemoryFilesystem
{
    return new MemoryFilesystem(new Mount($protocol));
}

/**
 * Create a new filesystem table with given filesystems.
 * Filesystems can be also mounted later.
 * If no filesystems are provided, local filesystem is mounted.
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function fstab(Filesystem ...$filesystems): FilesystemTable
{
    if (!count($filesystems)) {
        $filesystems[] = native_local_filesystem();
        $filesystems[] = stdout_filesystem();
        $filesystems[] = memory_filesystem();
    }

    return new FilesystemTable(...$filesystems);
}

/**
 * Wrap a filesystem with telemetry tracing support.
 * All filesystem and stream operations will be traced according to the configuration.
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function traceable_filesystem(Filesystem $filesystem, FilesystemTelemetryConfig $telemetryConfig): TraceableFilesystem
{
    return new TraceableFilesystem($filesystem, $telemetryConfig);
}

/**
 * Create a telemetry configuration for the filesystem.
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function filesystem_telemetry_config(
    Telemetry $telemetry,
    ClockInterface $clock,
    ?FilesystemTelemetryOptions $options = null,
): FilesystemTelemetryConfig {
    return new FilesystemTelemetryConfig($telemetry, $clock, $options ?? new FilesystemTelemetryOptions());
}

/**
 * Create options for filesystem telemetry.
 *
 * @param bool $trace_streams Create a single span per stream lifecycle (default: ON)
 * @param bool $collect_metrics Collect metrics for bytes/operation counts (default: ON)
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function filesystem_telemetry_options(
    bool $trace_streams = true,
    bool $collect_metrics = true,
): FilesystemTelemetryOptions {
    return new FilesystemTelemetryOptions($trace_streams, $collect_metrics);
}

/**
 * Copy a file from one path to another, across any filesystems mounted in the table.
 * Always streams bytes; same-filesystem copies do not use server-side optimizations
 * because `Filesystem::mv` is a move, not a copy.
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function file_copy(FilesystemTable $table, ?OperationOptions $options = null): Copy
{
    return new Copy($table, $options ?? new OperationOptions());
}

/**
 * Move a file from one path to another, across any filesystems mounted in the table.
 * Intra-filesystem moves delegate to `Filesystem::mv` for server-side optimizations;
 * cross-filesystem moves stream-copy then remove the source (non-atomic).
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function file_move(FilesystemTable $table, ?OperationOptions $options = null): Move
{
    return new Move($table, $options ?? new OperationOptions());
}

/**
 * Options shared by filesystem operations.
 *
 * @param int $chunkSize Number of bytes read/written per iteration when streaming across filesystems (default: 8192)
 */
#[DocumentationDSL(module: Module::FILESYSTEM, type: Type::HELPER)]
function operation_options(int $chunkSize = 8192): OperationOptions
{
    return new OperationOptions($chunkSize);
}
