<?php

declare(strict_types=1);

namespace Flow\Arrow\Parquet;

use Flow\Filesystem\DestinationStream;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

final class RowsWriter
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
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * Buffered rows, the footer, then the stream closed.
     */
    public function close(): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * An Arrow C Data struct batch, exported by an extension: its children by writer column name, a writer column
     * the batch lacks is nulls, a child the writer lacks is ignored.
     */
    public function writeBatch(object $batch): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * @param array<string, list<mixed>> $columns by top-level column name, every list of one length; a writer column
     *                                            the array lacks is nulls, a key the writer lacks is ignored
     */
    public function writeColumns(array $columns): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * @param array<array-key, mixed> $row a writer column the row lacks is null
     */
    public function writeRow(array $row): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * @param list<array<array-key, mixed>> $rows
     */
    public function writeRows(array $rows): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }
}
