<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel;

use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\RowGroup;

final readonly class RowGroupContainer
{
    /**
     * @param list<?ColumnIndex> $columnIndexes one per column chunk of the row group, in order
     * @param list<OffsetIndex> $offsetIndexes one per column chunk of the row group, in order
     */
    public function __construct(
        public string $binaryBuffer,
        public RowGroup $rowGroup,
        public array $columnIndexes,
        public array $offsetIndexes,
    ) {}
}
