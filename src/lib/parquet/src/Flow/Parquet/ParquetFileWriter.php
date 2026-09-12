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
     * @param iterable<array<string, mixed>> $rows
     */
    public function writeBatch(iterable $rows): void;

    /**
     * @param array<string, mixed> $row
     */
    public function writeRow(array $row): void;
}
