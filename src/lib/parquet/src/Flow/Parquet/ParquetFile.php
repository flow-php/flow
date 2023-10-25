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
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Thrift\FileMetaData;
use Flow\Parquet\ThriftStream\TPhpFileStream;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Thrift\Protocol\TCompactProtocol;

final class ParquetFile
{
    public const PARQUET_MAGIC_NUMBER = 'PAR1';

    private ?Metadata $metadata = null;

    /**
     * @param resource $stream
     */
    public function __construct(
        private                          $stream,
        private readonly ByteOrder $byteOrder,
        private readonly DataConverter $dataConverter,
        private readonly LoggerInterface $logger = new NullLogger()
    ) {
    }

    public function __destruct()
    {
        \fclose($this->stream);
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

    public function readChunks(FlatColumn $column, int $limit = null) : \Generator
    {
        $reader = new WholeChunkReader(
            new DataBuilder($this->dataConverter, $this->logger),
            new PageReader($column, $this->byteOrder, $this->logger),
            $this->logger
        );

        foreach ($this->getColumnChunks($column) as $columnChunks) {
            foreach ($columnChunks as $columnChunk) {
                $yieldedRows = 0;

                /** @var array $row */
                foreach ($reader->read($columnChunk, $column, $this->stream, $limit) as $row) {
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
    public function values(array $columns = [], int $limit = null) : \Generator
    {
        if (!\count($columns)) {
            $columns = \array_map(static fn (Column $c) => $c->name(), $this->schema()->columns());
        }

        $this->logger->info('[Parquet File] Reading values from columns', ['columns' => $columns]);

        foreach ($columns as $columnName) {
            if (!$this->metadata()->schema()->has($columnName)) {
                throw new InvalidArgumentException("Column \"{$columnName}\" does not exist");
            }
        }

        $rows = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

        foreach ($columns as $columnName) {
            $rows->attachIterator($this->read($this->schema()->get($columnName), $limit), $columnName);
        }

        /** @var array<string, mixed> $row */
        foreach ($rows as $row) {
            yield $row;
        }
    }

    /**
     * @return \Generator<array<ColumnChunk>>
     */
    private function getColumnChunks(Column $column) : \Generator
    {
        foreach ($this->metadata()->rowGroups()->all() as $rowGroup) {
            $chunksInGroup = [];

            foreach ($rowGroup->columnChunks() as $columnChunk) {
                if ($columnChunk->flatPath() === $column->flatPath()) {
                    $chunksInGroup[] = $columnChunk;
                }
            }

            yield $chunksInGroup;
        }
    }

    private function read(Column $column, int $limit = null) : \Generator
    {
        if ($column instanceof FlatColumn) {
            $this->logger->info('[Parquet File][Read Column] reading flat column', ['path' => $column->flatPath()]);

            return $this->readFlat($column, $limit);
        }

        if ($column instanceof NestedColumn) {
            if ($column->isList()) {
                $this->logger->info('[Parquet File][Read Column] reading list', ['path' => $column->flatPath()]);

                return $this->readList($column, $limit);
            }

            if ($column->isMap()) {
                $this->logger->info('[Parquet File][Read Column] reading map', ['path' => $column->flatPath()]);

                return $this->readMap($column, $limit);
            }

            $this->logger->info('[Parquet File][Read Column] reading structure', ['path' => $column->flatPath()]);

            return $this->readStruct($column, limit: $limit);
        }

        throw new RuntimeException('Unknown column type');
    }

    private function readFlat(FlatColumn $column, int $limit = null) : \Generator
    {
        return $this->readChunks($column, $limit);
    }

    private function readList(NestedColumn $listColumn, int $limit = null) : \Generator
    {
        $this->logger->debug('[Parquet File][Read Column][List]', ['column' => $listColumn->normalize()]);
        $elementColumn = $listColumn->getListElement();

        if ($elementColumn instanceof FlatColumn) {
            $this->logger->info('[Parquet File][Read Column][List] reading list element as a flat column', ['path' => $elementColumn->flatPath()]);

            return $this->readFlat($elementColumn, $limit);
        }

        /** @var NestedColumn $elementColumn */
        if ($elementColumn->isList()) {
            $this->logger->info('[Parquet File][Read Column][List] reading list element as list', ['path' => $elementColumn->flatPath()]);

            return $this->readList($elementColumn, $limit);
        }

        if ($elementColumn->isMap()) {
            $this->logger->info('[Parquet File][Read Column][List] reading list element as a map', ['path' => $elementColumn->flatPath()]);

            return $this->readMap($elementColumn, $limit);
        }

        $this->logger->info('[Parquet File][Read Column][List] reading list element as a structure', ['path' => $elementColumn->flatPath()]);

        return $this->readStruct($elementColumn, isCollection: true, limit: $limit);
    }

    private function readMap(NestedColumn $mapColumn, int $limit = null) : \Generator
    {
        $this->logger->debug('[Parquet File][Read Column][Map]', ['column' => $mapColumn->normalize()]);

        $keysColumn = $mapColumn->getMapKeyColumn();
        $valuesColumn = $mapColumn->getMapValueColumn();

        $keys = $this->readFlat($keysColumn, $limit);

        $values = null;

        if ($valuesColumn instanceof FlatColumn) {
            $this->logger->info('[Parquet File][Read Column][Map] reading values as a flat column', ['path' => $mapColumn->flatPath()]);
            $values = $this->readFlat($valuesColumn, $limit);
        }

        /** @var NestedColumn $valuesColumn */
        if ($valuesColumn->isList()) {
            $this->logger->info('[Parquet File][Read Column][Map] reading values as a list', ['path' => $mapColumn->flatPath()]);
            $values = $this->readList($valuesColumn, $limit);
        }

        if ($valuesColumn->isMap()) {
            $this->logger->info('[Parquet File][Read Column][Map] reading values as a map', ['path' => $mapColumn->flatPath()]);
            $values = $this->readMap($valuesColumn, $limit);
        }

        if ($valuesColumn->isStruct()) {
            $this->logger->info('[Parquet File][Read Column][Map] reading values as a structure', ['path' => $mapColumn->flatPath()]);
            $values = $this->readStruct($valuesColumn, isCollection: true, limit: $limit);
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
    private function readStruct(NestedColumn $structColumn, bool $isCollection = false, int $limit = null) : \Generator
    {
        $childrenRowsData = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

        foreach ($structColumn->children() as $child) {
            if ($child instanceof FlatColumn) {
                $childrenRowsData->attachIterator($this->read($child, $limit), $child->flatPath());

                continue;
            }

            if ($child instanceof NestedColumn) {
                if ($child->isList()) {
                    $childrenRowsData->attachIterator($this->readList($child, $limit), $child->flatPath());

                    continue;
                }

                if ($child->isMap()) {
                    $childrenRowsData->attachIterator($this->readMap($child, $limit), $child->flatPath());

                    continue;
                }

                $childrenRowsData->attachIterator($this->readStruct($child, isCollection: $isCollection, limit: $limit), $child->flatPath());

                continue;
            }

            throw new RuntimeException('Unknown column type');
        }

        $this->logger->debug('[Parquet File][Read Column][Structure]', ['column' => $structColumn->normalize()]);

        foreach ($childrenRowsData as $childrenRowData) {
            if ($isCollection) {
                $structsCollection = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

                /** @var array<array-key, mixed> $childColumnValue */
                foreach ($childrenRowData as $childColumnPath => $childColumnValue) {
                    $childColumn = $this->schema()->get($childColumnPath);
                    $structsCollection->attachIterator(new \ArrayIterator($childColumnValue), $childColumn->name());
                }

                $structs = [];

                foreach ($structsCollection as $structData) {
                    $structs[] = $structData;
                }

                yield $structs;
            } else {
                $row = [];

                foreach ($childrenRowData as $childColumnPath => $childColumnValue) {
                    $childColumn = $this->schema()->get($childColumnPath);

                    $row[$childColumn->name()] = $childColumnValue;
                }
                yield $row;
            }
        }
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    private function viewChunksPages(FlatColumn $column) : \Generator
    {
        $viewer = new WholeChunkViewer($this->logger);

        foreach ($this->getColumnChunks($column) as $columnChunks) {
            foreach ($columnChunks as $columnChunk) {
                foreach ($viewer->view($columnChunk, $column, $this->stream) as $pageHeader) {
                    yield new ColumnPageHeader($column, $columnChunk, $pageHeader);
                }
            }
        }
    }
}
