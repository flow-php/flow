<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset;

use Flow\ETL\Dataset\Statistics\ExecutionTime;

final readonly class Statistics
{
    public function __construct(
        private int $totalRows,
        public readonly ExecutionTime $executionTime,
    ) {
    }

    public function totalRows() : int
    {
        return $this->totalRows;
    }
}
