<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Exception\{InvalidArgumentException};
use Flow\Parquet\ParquetFile\ColumnChunkReader\WholeChunkReader;
use Flow\Parquet\ParquetFile\ColumnChunkViewer\WholeChunkViewer;
use Flow\Parquet\ParquetFile\RowGroup\FlowColumnChunk;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};
use Flow\Parquet\ParquetFile\{ColumnPageHeader,
    Metadata,
    PageReader,
    RowGroupBuilder\DremelAssembler,
    RowGroupBuilder\FlatColumnData,
    Schema};
use Flow\Parquet\Thrift\FileMetaData;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TMemoryBuffer;

final class ParquetFile
{
    public const PARQUET_MAGIC_NUMBER = 'PAR1';

    private DremelAssembler $dremelAssembler;

    private ?Metadata $metadata = null;

    public function __construct(
        private SourceStream $stream,
        private readonly ByteOrder $byteOrder,
        private readonly DataConverter $dataConverter,
        private readonly Options $options,
    ) {
        $this->dremelAssembler = new DremelAssembler($this->dataConverter);
    }

    public function __destruct()
    {
        $this->stream->close();
    }

    public function metadata() : Metadata
    {
        if ($this->metadata !== null) {
            return $this->metadata;
        }

        if ($this->stream->read(4, -4) !== self::PARQUET_MAGIC_NUMBER) {
            throw new InvalidArgumentException('Given file is not valid Parquet file');
        }

        /**
         * @phpstan-ignore-next-line
         */
        $metadataLength = \unpack($this->byteOrder->value, $this->stream->read(4, -8))[1];

        $metadata = $this->stream->read($metadataLength, -($metadataLength + 8));

        $thriftMetadata = new FileMetaData();
        $thriftMetadata->read(
            new TCompactProtocol(
                new TMemoryBuffer($metadata)
            )
        );

        $this->metadata = Metadata::fromThrift($thriftMetadata, $this->options);

        return $this->metadata;
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    public function pageHeaders() : \Generator
    {
        foreach ($this->schema()->columnsFlat() as $column) {
            foreach ($this->viewChunksPages($column) as $pageHeader) {
                yield $pageHeader;
            }
        }
    }

    public function readChunks(FlatColumn $column, ?int $limit = null, ?int $offset = null) : \Generator
    {
        $reader = new WholeChunkReader(
            new PageReader($this->byteOrder, $this->options),
            $this->options
        );

        $yieldedRows = 0;
        $skippedRows = 0;

        /** @var FlowColumnChunk $columnChunk */
        foreach ($this->getColumnChunks($column, offset: $offset) as $columnChunk) {
            $skipRows = $offset - $columnChunk->rowsOffset;

            /** @var array $row */
            foreach ($reader->read($columnChunk->chunk, $column, $this->stream) as $data) {
                foreach ($data->splitByRows(1) as $row) {
                    if ($skipRows >= 0 && $skipRows > $skippedRows) {
                        $skippedRows++;

                        continue;
                    }

                    yield $row;
                    $yieldedRows++;

                    if ($limit !== null && $yieldedRows >= $limit) {
                        return;
                    }
                }
            }
        }
    }

    public function schema() : Schema
    {
        return $this->metadata()->schema();
    }

    /**
     * @param array<string> $columns
     *
     * @return \Generator<int, array<string, mixed>>
     */
    public function values(array $columns = [], ?int $limit = null, ?int $offset = null) : \Generator
    {
        if ($limit !== null && $limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        if ($limit !== null && $offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than or equal to 0');
        }

        if (!\count($columns)) {
            $columns = \array_map(static fn (Column $c) => $c->name(), $this->schema()->columns());
        }

        foreach ($columns as $columnName) {
            if (!$this->metadata()->schema()->has($columnName)) {
                throw new InvalidArgumentException("Column \"{$columnName}\" does not exist");
            }
        }

        $rows = new \MultipleIterator();

        foreach ($columns as $columnName) {
            $rows->attachIterator($this->read($this->schema()->get($columnName), $limit, $offset), $columnName);
        }

        /** @var array<string, mixed> $row */
        foreach ($rows as $row) {
            yield \array_merge(...$row);
        }
    }

    /**
     * @return \Generator<FlowColumnChunk>
     */
    private function getColumnChunks(Column $column, ?int $offset = null) : \Generator
    {
        $fetchedRows = 0;

        foreach ($this->metadata()->rowGroups()->all() as $rowGroup) {
            if ($offset !== null) {

                if ($fetchedRows + $rowGroup->rowsCount() < $offset) {
                    $fetchedRows += $rowGroup->rowsCount();

                    continue;
                }
            }

            foreach ($rowGroup->columnChunks() as $columnChunk) {
                if ($columnChunk->flatPath() === $column->flatPath()) {
                    yield new FlowColumnChunk($columnChunk, $fetchedRows, $rowGroup->rowsCount());
                    $fetchedRows += $rowGroup->rowsCount();

                    break;
                }
            }
        }
    }

    private function read(Column $column, ?int $limit = null, ?int $offset = null) : \Generator
    {
        $columnData = FlatColumnData::initialize($column);

        if ($column instanceof FlatColumn) {
            foreach ($this->readChunks($column, $limit, $offset) as $data) {
                $columnData->addValue(...$data->iterator());
            }

            foreach ($this->dremelAssembler->assemble($column, $columnData) as $row) {
                yield $row;
            }

            return;
        }

        /**
         * @var NestedColumn $column
         */
        foreach ($column->childrenFlat() as $child) {
            foreach ($this->readChunks($child, $limit, $offset) as $data) {
                $columnData->addValues($data);
            }
        }

        foreach ($this->dremelAssembler->assemble($column, $columnData) as $row) {
            yield $row;
        }
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    private function viewChunksPages(FlatColumn $column) : \Generator
    {
        $viewer = new WholeChunkViewer($this->options);

        foreach ($this->getColumnChunks($column) as $columnChunk) {
            foreach ($viewer->view($columnChunk->chunk, $column, $this->stream) as $pageHeader) {
                yield new ColumnPageHeader($column, $columnChunk->chunk, $pageHeader);
            }
        }
    }
}
