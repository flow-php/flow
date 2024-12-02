<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData;

final class NullLevel
{
    public function __construct(public readonly int $level = 0)
    {
    }
}
