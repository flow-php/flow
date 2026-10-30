<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Metrics;

use Flow\ETL\Runtime\Metrics\Statistics\Columns;
use Flow\ETL\Runtime\Metrics\Statistics\ExecutionTime;
use Flow\ETL\Values\Memory\Consumption;

final readonly class Statistics
{
    public function __construct(
        private int $totalRows,
        public ExecutionTime $executionTime,
        public Consumption $memory,
        public ?Columns $columns,
    ) {}

    public function totalRows(): int
    {
        return $this->totalRows;
    }
}
