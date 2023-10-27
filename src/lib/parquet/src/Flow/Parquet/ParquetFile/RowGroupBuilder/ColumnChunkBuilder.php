<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class ColumnChunkBuilder
{
    private array $rows = [];

    private ColumnChunkStatistics $statistics;

    public function __construct(
        private readonly FlatColumn $column,
        private readonly DataConverter $dataConverter,
        private readonly PageSizeCalculator $calculator
    ) {
        $this->statistics = new ColumnChunkStatistics($column);
    }

    public function addRow(mixed $row) : void
    {
        $this->statistics->add($row);
        $this->rows[] = $row;
    }

    public function flush(int $fileOffset) : ColumnChunkContainer
    {
        $pageContainers = (new PagesBuilder($this->dataConverter, $this->calculator))->build($this->column, $this->rows, $this->statistics);

        $this->statistics->reset();

        return new ColumnChunkContainer(
            $pageContainers->buffer(),
            new ColumnChunk(
                type: $this->column->type(),
                codec: Compressions::UNCOMPRESSED,
                valuesCount: $pageContainers->valuesCount(),
                fileOffset: $fileOffset,
                path: $this->column->path(),
                encodings: $pageContainers->encodings(),
                totalCompressedSize: $pageContainers->size(),
                totalUncompressedSize: $pageContainers->size(),
                dictionaryPageOffset: ($pageContainers->dictionaryPageContainer()) ? $fileOffset : null,
                dataPageOffset: ($pageContainers->dictionaryPageContainer()) ? $fileOffset + $pageContainers->dictionaryPageContainer()->size() : $fileOffset,
                indexPageOffset: null,
            )
        );
    }

    public function statistics() : ColumnChunkStatistics
    {
        return $this->statistics;
    }
}
