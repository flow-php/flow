<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\ParquetFileWriter;

/**
 * Refuses every stream, as an engine does for a file that is not Parquet.
 */
final class RefusingParquetEngine implements ParquetEngine
{
    public function openForRead(SourceStream $stream): ParquetFileReader
    {
        throw new RuntimeException('not a Parquet file');
    }

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter {
        throw new RuntimeException('not a Parquet file');
    }

    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void {
        throw new RuntimeException('not a Parquet file');
    }
}
