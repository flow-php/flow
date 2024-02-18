<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\ColumnChunkReader\WholeChunkReader;
use Flow\Parquet\ParquetFile\ColumnChunkViewer\WholeChunkViewer;
use Flow\Parquet\ParquetFile\ColumnPageHeader;
use Flow\Parquet\ParquetFile\Data\DataBuilder;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\PageReader;
use Flow\Parquet\ParquetFile\RowGroup\FlowColumnChunk;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Thrift\FileMetaData;
use Flow\Parquet\ThriftStream\TPhpFileStream;
use Thrift\Protocol\TCompactProtocol;

final class ParquetFile
{
    public const PARQUET_MAGIC_NUMBER = 'PAR1';

    private ?Metadata $metadata = null;

    /**
     * @param resource $stream
     */
    public function __construct(
        private $stream,
        private readonly ByteOrder $byteOrder,
        private readonly DataConverter $dataConverter,
        private readonly Options $options
    ) {
    }

    public function __destruct()
    {
        /** @psalm-suppress RedundantConditionGivenDocblockType */
        if (\is_resource($this->stream)) {
            \fclose($this->stream);
        }
    }

    public function metadata() : Metadata
    {
        if ($this->metadata !== null) {
            return $this->metadata;
        }

        \fseek($this->stream, -4, SEEK_END);

        if (\fread($this->stream, 4) !== self::PARQUET_MAGIC_NUMBER) {
            throw new InvalidArgumentException('Given file is not valid Parquet file');
        }

        \fseek($this->stream, -8, SEEK_END);

        /**
         * @phpstan-ignore-next-line
         */
        $metadataLength = \unpack($this->byteOrder->value, \fread($this->stream, 4))[1];
        \fseek($this->stream, -($metadataLength + 8), SEEK_END);

        $thriftMetadata = new FileMetaData();
        $thriftMetadata->read(new TCompactProtocol(new TPhpFileStream($this->stream)));

        $this->metadata = Metadata::fromThrift($thriftMetadata);

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
            new DataBuilder($this->dataConverter),
            new PageReader($this->byteOrder, $this->options),
        );

        $yieldedRows = 0;
        $skippedRows = 0;

        /** @var FlowColumnChunk $columnChunk */
        foreach ($this->getColumnChunks($column, offset: $offset) as $columnChunk) {
            $skipRows = $offset - $columnChunk->rowsOffset;

            /** @var array $row */
            foreach ($reader->read($columnChunk->chunk, $column, $this->stream) as $row) {
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

        $rows = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

        foreach ($columns as $columnName) {
            $rows->attachIterator($this->read($this->schema()->get($columnName), $limit, $offset), $columnName);
        }

        /** @var array<string, mixed> $row */
        foreach ($rows as $row) {
            yield $row;
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
        if ($column instanceof FlatColumn) {
            return $this->readFlat($column, $limit, $offset);
        }

        if ($column instanceof NestedColumn) {
            if ($column->isList()) {
                return $this->readList($column, $limit, $offset);
            }

            if ($column->isMap()) {
                return $this->readMap($column, $limit, $offset);
            }

            return $this->readStruct($column, limit: $limit, offset: $offset);
        }

        throw new RuntimeException('Unknown column type');
    }

    private function readFlat(FlatColumn $column, ?int $limit = null, ?int $offset = null) : \Generator
    {
        return $this->readChunks($column, $limit, $offset);
    }

    private function readList(NestedColumn $listColumn, ?int $limit = null, ?int $offset = null) : \Generator
    {
        $elementColumn = $listColumn->getListElement();

        if ($elementColumn instanceof FlatColumn) {
            return $this->readFlat($elementColumn, $limit, $offset);
        }

        /** @var NestedColumn $elementColumn */
        if ($elementColumn->isList()) {
            return $this->readList($elementColumn, $limit, $offset);
        }

        if ($elementColumn->isMap()) {
            return $this->readMap($elementColumn, $limit, $offset);
        }

        return $this->readStruct($elementColumn, isCollection: true, limit: $limit, offset: $offset);
    }

    private function readMap(NestedColumn $mapColumn, ?int $limit = null, ?int $offset = null) : \Generator
    {
        $keysColumn = $mapColumn->getMapKeyColumn();
        $valuesColumn = $mapColumn->getMapValueColumn();

        $keys = $this->readFlat($keysColumn, $limit, $offset);

        $values = null;

        if ($valuesColumn instanceof FlatColumn) {
            $values = $this->readFlat($valuesColumn, $limit, $offset);
        }

        /** @var NestedColumn $valuesColumn */
        if ($valuesColumn->isList()) {
            $values = $this->readList($valuesColumn, $limit, $offset);
        }

        if ($valuesColumn->isMap()) {
            $values = $this->readMap($valuesColumn, $limit, $offset);
        }

        if ($valuesColumn->isStruct()) {
            $values = $this->readStruct($valuesColumn, isCollection: true, limit: $limit, offset: $offset);
        }

        if ($values === null) {
            throw new RuntimeException('Unknown column type');
        }

        $mapFlat = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);
        $mapFlat->attachIterator($keys, 'keys');
        $mapFlat->attachIterator($values, 'values');

        foreach ($mapFlat as $row) {
            if ($row['keys'] === null) {
                yield null;
            } else {
                if (\is_array($row['keys'])) {
                    yield \Flow\Parquet\array_combine_recursive($row['keys'], $row['values']);
                } else {
                    yield [$row['keys'] => $row['values']];
                }
            }
        }
    }

    /**
     * @param bool $isCollection - when structure is a map or list element, each struct child is a collection for example ['int' => [1, 2, 3]] instead of ['int' => 1]
     */
    private function readStruct(NestedColumn $structColumn, bool $isCollection = false, ?int $limit = null, ?int $offset = null) : \Generator
    {
        $childrenRowsData = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

        foreach ($structColumn->children() as $child) {
            if ($child instanceof FlatColumn) {
                $childrenRowsData->attachIterator($this->read($child, $limit, $offset), $child->flatPath());

                continue;
            }

            if ($child instanceof NestedColumn) {
                if ($child->isList()) {
                    $childrenRowsData->attachIterator($this->readList($child, $limit, $offset), $child->flatPath());

                    continue;
                }

                if ($child->isMap()) {
                    $childrenRowsData->attachIterator($this->readMap($child, $limit, $offset), $child->flatPath());

                    continue;
                }

                $childrenRowsData->attachIterator($this->readStruct($child, isCollection: $isCollection, limit: $limit, offset: $offset), $child->flatPath());

                continue;
            }

            throw new RuntimeException('Unknown column type');
        }

        foreach ($childrenRowsData as $childrenRowData) {
            if ($isCollection) {
                $structsCollection = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

                /** @var null|array<array-key, mixed> $childColumnValue */
                foreach ($childrenRowData as $childColumnPath => $childColumnValue) {
                    if ($childColumnValue !== null) {
                        $childColumn = $this->schema()->get($childColumnPath);
                        $structsCollection->attachIterator(new \ArrayIterator($childColumnValue), $childColumn->name());
                    }
                }

                $structs = null;

                foreach ($structsCollection as $structData) {
                    $structs[] = $structData;
                }

                yield $structs;
            } else {
                $row = [];

                $isNull = true;

                foreach ($childrenRowData as $childColumnPath => $childColumnValue) {
                    $childColumn = $this->schema()->get($childColumnPath);

                    $row[$childColumn->name()] = $childColumnValue;

                    if ($childColumnValue !== null) {
                        $isNull = false;
                    }
                }

                if ($isNull) {
                    yield null;
                } else {
                    yield $row;
                }
            }
        }
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    private function viewChunksPages(FlatColumn $column) : \Generator
    {
        $viewer = new WholeChunkViewer();

        foreach ($this->getColumnChunks($column) as $columnChunk) {
            foreach ($viewer->view($columnChunk->chunk, $column, $this->stream) as $pageHeader) {
                yield new ColumnPageHeader($column, $columnChunk->chunk, $pageHeader);
            }
        }
    }
}
