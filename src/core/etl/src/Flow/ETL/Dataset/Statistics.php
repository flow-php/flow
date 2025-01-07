<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset;

final readonly class Statistics
{
    public function __construct(
        private int $totalRows,
    ) {
    }

    public function totalRows() : int
    {
        return $this->totalRows;
    }
}
