<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\RowGroup;

final class RowGroupContainer
{
    public function __construct(
        public readonly string $binaryBuffer,
        public readonly RowGroup $rowGroup
    ) {
    }
}
