<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\ParquetFileWriter;

final readonly class AdaptiveParquetEngine implements ParquetEngine
{
    private ParquetEngine $engine;

    public function __construct(
        ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        Options $options = new Options(),
        ?ArrowExtension $extension = null,
    ) {
        $this->engine = ($extension ?? ArrowExtension::detect())->available() && $byteOrder === ByteOrder::LITTLE_ENDIAN
            ? new RustParquetEngine($options)
            : new PhpParquetEngine($byteOrder, $options);
    }

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
