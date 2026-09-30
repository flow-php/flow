<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\Schema;
use Generator;

interface ParquetFileReader
{
    /**
     * Closes the stream it was opened on; any later call throws RuntimeException('Reader is not open').
     */
    public function close(): void;

    /**
     * The whole footer as Metadata, built on the first call.
     */
    public function metadata(): Metadata;

    /**
     * @param list<string> $columns resolved names, never empty
     * @param int<1, max> $batchSize upper bound of rows per chunk
     *
     * @return Generator<int, array<string, list<mixed>>> chunks keyed by column, every list the same length (>0)
     */
    public function readColumns(array $columns, int $batchSize, ?int $limit, ?int $offset): Generator;

    public function rowsNumber(): int;

    /**
     * The file schema, without building the row-group metadata.
     */
    public function schema(): Schema;

    /**
     * Σ row group total_byte_size (uncompressed), without building the row-group metadata.
     */
    public function totalByteSize(): int;
}
