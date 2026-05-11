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

final class ParquetFile
{
    public const string PARQUET_MAGIC_NUMBER = 'PAR1';

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

        if ($this->stream->read(4, $fileTotalSize - 4) !== self::PARQUET_MAGIC_NUMBER) {
            throw new InvalidArgumentException('Given file is not valid Parquet file');
        }

        /**
         * @phpstan-ignore-next-line
         */
        $metadataLength = \unpack($this->byteOrder->value, $this->stream->read(4, $fileTotalSize - 8))[1];

        $metadata = $this->stream->read($metadataLength, $fileTotalSize - ($metadataLength + 8));

        $thriftMetadata = new FileMetaData();
        $thriftMetadata->read(new CompactProtocol(new MemoryBuffer($metadata)));

        $this->metadata = Metadata::fromThrift($thriftMetadata);

        return $this->metadata;
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    public function pageHeaders(): \Generator
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
     * @return \Generator<int, array<string, mixed>>
     */
    public function values(array $columns = [], ?int $limit = null, ?int $offset = null): \Generator
    {
        if ($limit !== null && $limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        if ($limit !== null && $offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than or equal to 0');
        }

        if (!\count($columns)) {
            $columns = \array_map(static fn(Column $c) => $c->name(), $this->schema()->columns());
        }

        foreach ($columns as $columnName) {
            if (!$this->metadata()->schema()->has($columnName)) {
                throw new InvalidArgumentException("Column \"{$columnName}\" does not exist");
            }
        }

        yield from $this->engine->readValues($this->stream, $this->schema(), $columns, $limit, $offset);
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    private function viewChunksPages(FlatColumn $column): \Generator
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
