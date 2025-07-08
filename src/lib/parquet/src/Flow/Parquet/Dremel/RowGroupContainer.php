<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel;

use Flow\Parquet\ParquetFile\RowGroup;

final readonly class RowGroupContainer
{
    public function __construct(
        public string $binaryBuffer,
        public RowGroup $rowGroup,
    ) {
    }
}
