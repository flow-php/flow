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
     * @param array<string> $columns
     *
     * @return \Generator<int, array<array-key, mixed>>
     */
    public function readValues(
        SourceStream $stream,
        Schema $schema,
        array $columns = [],
        ?int $limit = null,
        ?int $offset = null,
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
