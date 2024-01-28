<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset;

final class Statistics
{
    public function __construct(
        private readonly int $totalRows,
    ) {
    }

    public function totalRows() : int
    {
        return $this->totalRows;
    }
}
