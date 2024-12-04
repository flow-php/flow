<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\FlatColumnValues;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class ColumnChunkBuilder
{
    private FlatColumnValues $columnData;

    private ColumnChunkStatistics $statistics;

    public function __construct(
        private readonly FlatColumn $column,
        private readonly Compressions $compression,
        private readonly PageSizeCalculator $calculator,
        private readonly Options $options,
    ) {
        $this->statistics = new ColumnChunkStatistics($this->column);
        $this->columnData = new FlatColumnValues($this->column);
    }

    public function addRow(FlatColumnValues $row) : void
    {
        $this->columnData->merge($row);

        foreach ($row->values() as $value) {
            $this->statistics->add($value);
        }
    }

    public function flush(int $fileOffset) : ColumnChunkContainer
    {
        $pageContainers = (new PagesBuilder($this->compression, $this->calculator, $this->options))
            ->build($this->column, $this->columnData, $this->statistics);

        $statistics = (new StatisticsBuilder())->build($this->column, $this->statistics);

        $this->statistics->reset();

        return new ColumnChunkContainer(
            $pageContainers->buffer(),
            new ColumnChunk(
                type: $this->column->type(),
                codec: $this->compression,
                valuesCount: $pageContainers->valuesCount(),
                fileOffset: $fileOffset,
                path: $this->column->path(),
                encodings: $pageContainers->encodings(),
                totalCompressedSize: $pageContainers->compressedSize(),
                totalUncompressedSize: $pageContainers->uncompressedSize(),
                dictionaryPageOffset: ($pageContainers->dictionaryPageContainer()) ? $fileOffset : null,
                dataPageOffset: ($pageContainers->dictionaryPageContainer()) ? $fileOffset + $pageContainers->dictionaryPageContainer()->totalCompressedSize() : $fileOffset,
                indexPageOffset: null,
                statistics: $statistics,
                options: $this->options
            )
        );
    }

    public function rows() : FlatColumnValues
    {
        return $this->columnData;
    }

    public function statistics() : ColumnChunkStatistics
    {
        return $this->statistics;
    }
}
