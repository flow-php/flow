<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Generator;

interface ParquetEngine
{
    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter;

    /**
     * @param list<string> $columns resolved names, never empty
     * @param int<1, max> $batchSize upper bound of rows per chunk
     *
     * @return Generator<int, array<string, list<mixed>>> chunks keyed by column, every list the same length (>0)
     */
    public function readColumns(
        SourceStream $stream,
        Schema $schema,
        array $columns,
        int $batchSize,
        ?int $limit,
        ?int $offset,
    ): Generator;

    /**
     * @param iterable<array<array-key, mixed>> $rows
     */
    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void;
}
