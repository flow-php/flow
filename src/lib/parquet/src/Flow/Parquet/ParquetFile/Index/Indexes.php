<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Index;

use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface Indexes
{
    /**
     * Null when the chunk has no column index.
     */
    public function columnIndex(int $rowGroup, FlatColumn $column): ?ColumnIndex;

    /**
     * Null when the chunk has no offset index.
     */
    public function offsetIndex(int $rowGroup, FlatColumn $column): ?OffsetIndex;
}
