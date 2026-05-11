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

final readonly class AdaptiveParquetEngine implements ParquetEngine
{
    private ParquetEngine $delegate;

    public function __construct(ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN, Options $options = new Options())
    {
        $this->delegate = \extension_loaded('arrow')
            ? new ArrowParquetEngine($options)
            : new PhpParquetEngine($byteOrder, $options);
    }

    public function closeWrite(): void
    {
        $this->delegate->closeWrite();
    }

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): void {
        $this->delegate->openForWrite($stream, $schema, $compression, $options);
    }

    public function readValues(
        SourceStream $stream,
        Schema $schema,
        array $columns = [],
        ?int $limit = null,
        ?int $offset = null,
    ): \Generator {
        return $this->delegate->readValues($stream, $schema, $columns, $limit, $offset);
    }

    public function writeBatch(iterable $rows): void
    {
        $this->delegate->writeBatch($rows);
    }

    public function writeRow(array $row): void
    {
        $this->delegate->writeRow($row);
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
