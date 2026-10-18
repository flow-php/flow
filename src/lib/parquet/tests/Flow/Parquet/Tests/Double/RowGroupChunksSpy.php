<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Double;

use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

/**
 * Records the row group of each chunk read.
 */
final class RowGroupChunksSpy implements RowGroupChunks
{
    /**
     * @var list<int>
     */
    public array $chunkReads = [];

    public function __construct(
        private readonly RowGroupChunks $chunks,
    ) {}

    public function chunk(int $rowGroup, FlatColumn $column): ColumnChunk
    {
        $this->chunkReads[] = $rowGroup;

        return $this->chunks->chunk($rowGroup, $column);
    }

    public function columnOrder(FlatColumn $column): ColumnOrder
    {
        return $this->chunks->columnOrder($column);
    }

    public function count(): int
    {
        return $this->chunks->count();
    }

    public function rowCount(int $rowGroup): int
    {
        return $this->chunks->rowCount($rowGroup);
    }

    public function rowCounts(): array
    {
        return $this->chunks->rowCounts();
    }
}
