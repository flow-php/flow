<?php

declare(strict_types=1);

namespace Flow\Parquet;

interface ParquetFileWriter
{
    /**
     * Writes what is buffered and the footer, then closes the stream it was opened on. The writer is closed
     * afterwards even when this throws; any later call throws RuntimeException('Writer is not open').
     */
    public function close(): void;

    /**
     * @param iterable<array<array-key, mixed>> $rows
     */
    public function writeBatch(iterable $rows): void;

    /**
     * @param array<string, list<mixed>> $columns by top-level column name, every list of one length; a schema column
     *                                            the array lacks is written as nulls, a key the schema lacks is ignored
     */
    public function writeColumns(array $columns): void;

    /**
     * @param array<array-key, mixed> $row
     */
    public function writeRow(array $row): void;
}
