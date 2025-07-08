<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel\ColumnData;

final readonly class NullLevel
{
    public function __construct(public int $level = 0)
    {
    }
}
