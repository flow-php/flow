<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

final readonly class FlowColumnChunk
{
    public function __construct(
        public ColumnChunk $chunk,
        public int $rowsOffset,
        public int $rowsInGroup,
    ) {
    }
}
