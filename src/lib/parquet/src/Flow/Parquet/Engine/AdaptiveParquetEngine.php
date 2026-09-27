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
use Flow\Parquet\ParquetFileWriter;
use Generator;

use function extension_loaded;

final readonly class AdaptiveParquetEngine implements ParquetEngine
{
    private ParquetEngine $delegate;

    public function __construct(ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN, Options $options = new Options())
    {
        $this->delegate = extension_loaded('arrow')
            ? new ArrowParquetEngine($options)
            : new PhpParquetEngine($byteOrder, $options);
    }

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter {
        return $this->delegate->openForWrite($stream, $schema, $compression, $options);
    }

    public function readColumns(
        SourceStream $stream,
        Schema $schema,
        array $columns,
        int $batchSize,
        ?int $limit,
        ?int $offset,
    ): Generator {
        return $this->delegate->readColumns($stream, $schema, $columns, $batchSize, $limit, $offset);
    }

    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void {
        $this->delegate->writeRows($stream, $schema, $compression, $options, $rows);
    }
}
