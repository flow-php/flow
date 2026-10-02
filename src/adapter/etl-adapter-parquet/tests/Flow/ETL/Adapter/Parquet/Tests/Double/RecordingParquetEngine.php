<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\ParquetFileWriter;

final class RecordingParquetEngine implements ParquetEngine
{
    public int $openedForWrite = 0;

    public function __construct(
        private readonly ParquetEngine $engine = new PhpParquetEngine(),
    ) {}

    public function openForRead(SourceStream $stream): ParquetFileReader
    {
        return $this->engine->openForRead($stream);
    }

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter {
        $this->openedForWrite++;

        return $this->engine->openForWrite($stream, $schema, $compression, $options);
    }

    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void {
        $this->engine->writeRows($stream, $schema, $compression, $options, $rows);
    }
}
