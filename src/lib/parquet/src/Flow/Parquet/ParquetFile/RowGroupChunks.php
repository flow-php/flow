<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface RowGroupChunks
{
    public function chunk(int $rowGroup, FlatColumn $column): ColumnChunk;

    public function columnOrder(FlatColumn $column): ColumnOrder;

    public function count(): int;

    public function rowCount(int $rowGroup): int;

    /**
     * Every row group's row count, in file order.
     *
     * @return list<int>
     */
    public function rowCounts(): array;
}
