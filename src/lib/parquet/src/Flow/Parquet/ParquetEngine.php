<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;

interface ParquetEngine
{
    /**
     * The file on $stream, its footer read by the returned reader, which owns the stream from here on.
     */
    public function openForRead(SourceStream $stream): ParquetFileReader;

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter;

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
