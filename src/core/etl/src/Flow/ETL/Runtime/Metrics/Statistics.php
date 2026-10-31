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
        public Consumption $heap,
        public ?Columns $columns,
        public Processes $processes,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function normalize(): array
    {
        return [
            'total_rows' => $this->totalRows,
            'execution_time' => $this->executionTime->normalize(),
            'heap' => $this->heap->normalize(),
            'processes' => $this->processes->normalize(),
            'columns' => $this->columns?->normalize(),
        ];
    }

    public function totalRows(): int
    {
        return $this->totalRows;
    }
}
