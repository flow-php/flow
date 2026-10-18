<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Index;

use Flow\Parquet\BloomFilter\Sbbf;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface Indexes
{
    /**
     * Null when the chunk has no bloom filter, or one of an algorithm, hash or compression this reader does not support.
     */
    public function bloomFilter(int $rowGroup, FlatColumn $column): ?Sbbf;

    /**
     * Null when the chunk has no column index.
     */
    public function columnIndex(int $rowGroup, FlatColumn $column): ?ColumnIndex;

    /**
     * Null when the chunk has no offset index.
     */
    public function offsetIndex(int $rowGroup, FlatColumn $column): ?OffsetIndex;
}
