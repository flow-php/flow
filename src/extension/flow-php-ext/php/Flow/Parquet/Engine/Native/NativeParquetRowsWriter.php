<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine\Native;

use Flow\Filesystem\DestinationStream;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class NativeParquetRowsWriter
{
    /**
     * @param array<array-key, mixed> $schema Flow\Parquet\Engine\Arrow\SchemaConverter::toExtension()
     * @param array<string, mixed> $options Flow\Parquet\Engine\Arrow\OptionsConverter::toExtension()
     * @param int $batchSize rows per written batch; below 1 is refused
     */
    public function __construct(
        DestinationStream $stream,
        array $schema,
        string $compression,
        array $options,
        int $batchSize,
    ) {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * Buffered rows, the footer, then the stream closed.
     */
    public function close(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param array<array-key, mixed> $row a writer column the row lacks is null
     */
    public function writeRow(array $row): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param list<array<array-key, mixed>> $rows
     */
    public function writeRows(array $rows): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
