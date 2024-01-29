<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

final class FlowColumnChunk
{
    public function __construct(
        public readonly ColumnChunk $chunk,
        public readonly int $rowsOffset,
        public readonly int $rowsInGroup,
    ) {
    }
}
