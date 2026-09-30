<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\ParquetFileWriter;

final class PhpParquetEngine implements ParquetEngine
{
    public function __construct(
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        private readonly Options $options = new Options(),
    ) {}

    public function openForRead(SourceStream $stream): ParquetFileReader
    {
        return new PhpParquetFileReader($stream, $this->byteOrder, $this->options);
    }

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter {
        return new PhpParquetFileWriter(
            $stream,
            $schema,
            $compression,
            $options,
            $this->options->getInt(Option::ROW_GROUP_SIZE_CHECK_INTERVAL),
        );
    }

    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void {
        // writeRows() reads the row group check interval from its own $options, openForWrite() from the engine's
        $file = new PhpParquetFileWriter(
            $stream,
            $schema,
            $compression,
            $options,
            $options->getInt(Option::ROW_GROUP_SIZE_CHECK_INTERVAL),
        );

        foreach ($rows as $row) {
            $file->writeRow($row);
        }

        $file->close();
    }
}
