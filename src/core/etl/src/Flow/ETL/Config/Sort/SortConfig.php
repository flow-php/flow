<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Sort\SortAlgorithms;

final readonly class SortConfig
{
    public const SORT_MAX_MEMORY_ENV = 'FLOW_SORT_MAX_MEMORY';

    public function __construct(
        public SortAlgorithms $algorithm,
        public Unit $memoryLimit,
    ) {
    }
}
