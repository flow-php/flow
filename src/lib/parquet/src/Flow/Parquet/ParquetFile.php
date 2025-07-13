<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\{Dremel\ColumnData\ReadFlatColumnValues,
    Dremel\DremelAssembler,
    Dremel\ReadColumnData,
    ParquetFile\Metadata,
    ParquetFile\Page\ColumnPageHeader,
    ParquetFile\Schema,
    Reader\PageReader};
use Flow\Parquet\Exception\{InvalidArgumentException, RuntimeException};
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn};
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader\{ColumnChunkReader, ColumnChunkViewer};
use Flow\Parquet\Thrift\FileMetaData;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TMemoryBuffer;

final class ParquetFile
{
    public const PARQUET_MAGIC_NUMBER = 'PAR1';

    private readonly DremelAssembler $dremelAssembler;

    private ?Metadata $metadata = null;

    public function __construct(
        private readonly SourceStream $stream,
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

        $totalRows = $this->metadata()->rowsNumber();

        if ($offset > $totalRows) {
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

        $multipleIterator = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

        foreach ($columns as $columnName) {
            $multipleIterator->attachIterator($this->read($this->schema()->get($columnName), $limit, $offset), $columnName);
        }

        $rowCount = 0;

        foreach ($multipleIterator as $rowData) {
            if ($limit !== null && $rowCount >= $limit) {
                break;
            }

            $row = [];

            foreach ($rowData as $columnData) {
                if ($columnData !== null) {
                    foreach ($columnData as $key => $value) {
                        $row[$key] = $value;
                    }
                }
            }

            yield $row;
            $rowCount++;
        }
    }

    private function read(Column $column, ?int $limit = null, ?int $offset = null) : \Generator
    {
        $yieldedRows = 0;
        $rowGroupOffset = 0;
        $chunkReader = new ColumnChunkReader(
            new PageReader($this->byteOrder, $this->options),
            $this->options
        );

        foreach ($this->metadata()->rowGroups()->all() as $rowGroup) {
            if ($offset !== null) {

                if ($rowGroupOffset + $rowGroup->rowsCount() <= $offset) {
                    $rowGroupOffset += $rowGroup->rowsCount();

                    continue;
                }
            }
            $skipRows = $offset - $rowGroupOffset;

            if ($column instanceof FlatColumn) {
                foreach ($chunkReader->read($rowGroup->getColumnChunk($column), $column, $this->stream) as $flatColumnValues) {
                    $columnData = new ReadColumnData($column, [$flatColumnValues->flatPath() => $flatColumnValues]);

                    $rowsSkipped = 0;

                    foreach ($this->dremelAssembler->assemble($column, $columnData) as $row) {
                        if ($skipRows > 0 && $rowsSkipped < $skipRows) {
                            $rowsSkipped++;

                            continue;
                        }

                        if ($limit !== null && $yieldedRows >= $limit) {
                            return;
                        }
                        yield $row;
                        $yieldedRows++;
                    }
                }
            } elseif ($column instanceof NestedColumn) {

                $childrenFlatValuesIterator = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

                foreach ($column->childrenFlat() as $child) {
                    $childrenFlatValuesIterator->attachIterator($chunkReader->read($rowGroup->getColumnChunk($child), $child, $this->stream), $child->flatPath());
                }

                foreach ($childrenFlatValuesIterator as $childrenFlatValues) {
                    $columnFlatData = [];

                    foreach ($childrenFlatValues as $flatPath => $childFlatValues) {
                        if (!$childFlatValues instanceof ReadFlatColumnValues) {
                            // The reason why this might happen is because when we are writing to parquet file
                            // we write each nested column child as a separate flat column.
                            // Now when the mechanism that calculates how many rows will fit in the page
                            // it's unaware of the fact that some of the columns should share the rows count with their siblings.
                            throw new RuntimeException('Unexpected child flat values');
                        }

                        $columnFlatData[$flatPath] = $childFlatValues;
                    }

                    $columnData = new ReadColumnData($column, \array_values($columnFlatData));

                    $rowsSkipped = 0;

                    foreach ($this->dremelAssembler->assemble($column, $columnData) as $row) {
                        if ($skipRows > 0 && $rowsSkipped < $skipRows) {
                            $rowsSkipped++;

                            continue;
                        }

                        if ($limit !== null && $yieldedRows >= $limit) {
                            return;
                        }
                        yield $row;
                        $yieldedRows++;
                    }
                }
            } else {
                throw new InvalidArgumentException('Column must be instance of FlatColumn or NestedColumn');
            }

            $rowGroupOffset += $rowGroup->rowsCount();
        }
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    private function viewChunksPages(FlatColumn $column) : \Generator
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
