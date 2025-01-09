<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData;

final readonly class NullLevel
{
    public function __construct(public int $level = 0)
    {
    }
}
