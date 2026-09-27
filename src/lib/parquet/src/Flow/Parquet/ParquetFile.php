<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\Page\ColumnPageHeader;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader\ColumnChunkViewer;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\FileMetaData;
use Generator;

use function array_keys;
use function array_map;
use function array_values;
use function count;
use function unpack;

final class ParquetFile
{
    public const string PARQUET_MAGIC_NUMBER = 'PAR1';

    private const int VALUES_BATCH_SIZE = 1024;

    private ?Metadata $metadata = null;

    public function __construct(
        private readonly SourceStream $stream,
        private readonly ByteOrder $byteOrder,
        private readonly Options $options,
        private readonly ParquetEngine $engine,
    ) {}

    public function __destruct()
    {
        $this->stream->close();
    }

    public function metadata(): Metadata
    {
        if ($this->metadata !== null) {
            return $this->metadata;
        }

        $fileTotalSize = $this->stream->size();

        if ($fileTotalSize === null) {
            throw new InvalidArgumentException('Cannot determine Parquet file size');
        }

        if ($this->stream->read(4, $fileTotalSize - 4) !== self::PARQUET_MAGIC_NUMBER) {
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
     * @return \Generator<ColumnPageHeader>
     */
    public function pageHeaders(): Generator
    {
        foreach ($this->schema()->columnsFlat() as $column) {
            foreach ($this->viewChunksPages($column) as $pageHeader) {
                yield $pageHeader;
            }
        }
    }

    public function schema(): Schema
    {
        return $this->metadata()->schema();
    }

    /**
     * @param array<string> $columns
     *
     * @return Generator<int, array<string, list<mixed>>>
     */
    public function columns(int $batchSize, array $columns = [], ?int $limit = null, ?int $offset = null): Generator
    {
        if ($batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0');
        }

        if ($limit !== null && $limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        if ($offset !== null && $offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than or equal to 0');
        }

        if (!count($columns)) {
            $columns = array_map(static fn(Column $c) => $c->name(), $this->schema()->columns());
        }

        foreach ($columns as $columnName) {
            if (!$this->metadata()->schema()->has($columnName)) {
                throw new InvalidArgumentException("Column \"{$columnName}\" does not exist");
            }
        }

        yield from $this->engine->readColumns(
            $this->stream,
            $this->schema(),
            array_values($columns),
            $batchSize,
            $limit,
            $offset,
        );
    }

    /**
     * @param array<string> $columns
     *
     * @return Generator<int, array<string, mixed>>
     */
    public function values(array $columns = [], ?int $limit = null, ?int $offset = null): Generator
    {
        foreach ($this->columns(self::VALUES_BATCH_SIZE, $columns, $limit, $offset) as $chunk) {
            $names = array_keys($chunk);
            $count = count($chunk[$names[0]]);

            for ($i = 0; $i < $count; $i++) {
                $row = [];

                foreach ($names as $name) {
                    $row[$name] = $chunk[$name][$i];
                }

                yield $row;
            }
        }
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    private function viewChunksPages(FlatColumn $column): Generator
    {
        $viewer = new ColumnChunkViewer($this->options);

        foreach ($this->metadata()->rowGroups()->all() as $rowGroup) {
            foreach ($rowGroup->columnChunks() as $columnChunk) {
                foreach ($viewer->view($columnChunk, $this->stream) as $pageHeader) {
                    yield new ColumnPageHeader($column, $columnChunk, $pageHeader);
                }
            }
        }
    }
}
