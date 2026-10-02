<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\Reader\ColumnChunkReader;
use Flow\Parquet\Reader\ColumnReader;
use Flow\Parquet\Reader\PageReader;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\FileMetaData;
use Generator;

use function count;
use function is_array;
use function min;
use function unpack;

final class PhpParquetFileReader implements ParquetFileReader
{
    private ?Metadata $metadata = null;

    private bool $open = true;

    public function __construct(
        private readonly SourceStream $stream,
        private readonly ByteOrder $byteOrder,
        private readonly Options $options,
    ) {}

    public function close(): void
    {
        $this->open || throw new RuntimeException('Reader is not open');

        $this->open = false;
        $this->stream->close();
    }

    public function metadata(): Metadata
    {
        $this->open || throw new RuntimeException('Reader is not open');

        if ($this->metadata !== null) {
            return $this->metadata;
        }

        $fileTotalSize = $this->stream->size();

        if ($fileTotalSize === null) {
            throw new InvalidArgumentException('Cannot determine Parquet file size');
        }

        if ($this->stream->read(4, $fileTotalSize - 4) !== ParquetFile::PARQUET_MAGIC_NUMBER) {
            throw new InvalidArgumentException('Given file is not valid Parquet file');
        }

        $unpacked = unpack($this->byteOrder->value, $this->stream->read(4, $fileTotalSize - 8));

        if ($unpacked === false) {
            throw new InvalidArgumentException('Failed to read Parquet metadata length');
        }

        $metadataLength = $unpacked[1];

        if ($metadataLength <= 0) {
            throw new InvalidArgumentException('Parquet metadata length must be positive, got ' . $metadataLength);
        }

        $metadata = $this->stream->read($metadataLength, $fileTotalSize - ($metadataLength + 8));

        $thriftMetadata = new FileMetaData();
        $thriftMetadata->read(new CompactProtocol(new MemoryBuffer($metadata)));

        $this->metadata = Metadata::fromThrift($thriftMetadata);

        return $this->metadata;
    }

    /**
     * @param list<string> $columns
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, array<string, list<mixed>>>
     */
    public function readColumns(array $columns, int $batchSize, ?int $limit, ?int $offset): Generator
    {
        $this->open || throw new RuntimeException('Reader is not open');

        $columnReader = new ColumnReader(
            new ColumnChunkReader(new PageReader($this->byteOrder, $this->options), $this->options),
            new DremelAssembler(DataConverter::initialize($this->options)),
        );

        $metadata = $this->metadata();

        $totalRows = $metadata->rowsNumber();

        if ($offset !== null && $offset > $totalRows) {
            return;
        }

        if ($offset !== null) {
            if ($totalRows > $offset) {
                $totalRows -= $offset;
            } else {
                $totalRows = 0;
            }
        }

        $totalRows = min($totalRows, $limit ?? $totalRows);

        if ($totalRows === 0) {
            return;
        }

        $generators = [];

        foreach ($columns as $columnName) {
            $generators[$columnName] = $columnReader->read(
                $metadata->schema()->get($columnName),
                $metadata,
                $this->stream,
                $limit,
                $offset,
            );
        }

        $remaining = $totalRows;

        while ($remaining > 0) {
            $take = min($batchSize, $remaining);
            $chunk = [];
            $read = 0;

            foreach ($generators as $name => $generator) {
                $values = [];

                while (count($values) < $take && $generator->valid()) {
                    // @mago-ignore analysis:mixed-assignment
                    $entry = $generator->current();

                    if (is_array($entry)) {
                        // @mago-ignore analysis:mixed-assignment
                        foreach ($entry as $value) {
                            $values[] = $value;
                        }
                    }

                    $generator->next();
                }

                $chunk[$name] = $values;
                $read = count($values);
            }

            if ($read === 0) {
                return;
            }

            yield $chunk;
            $remaining -= $read;
        }
    }

    public function rowsNumber(): int
    {
        return $this->metadata()->rowsNumber();
    }

    public function schema(): Schema
    {
        return $this->metadata()->schema();
    }

    public function totalByteSize(): int
    {
        $bytes = 0;

        foreach ($this->metadata()->rowGroups()->all() as $rowGroup) {
            $bytes += $rowGroup->totalByteSize();
        }

        return $bytes;
    }
}
