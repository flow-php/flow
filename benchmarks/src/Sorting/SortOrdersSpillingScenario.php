<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Sorting;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Datasets;
use Flow\ETL\Values\Memory\Unit;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\ref;
use function Flow\Floe\DSL\from_floe;
use function str_repeat;

final readonly class SortOrdersSpillingScenario
{
    public string $heldElsewhere;

    public function __construct(
        private int $rows,
    ) {
        // memory the rest of the process holds before the sort starts: the sort's limit counts only what it adds
        $this->heldElsewhere = str_repeat('x', 32 * 1024 * 1024);
    }

    public function run(): void
    {
        $config = BenchmarkConfig::builder()->sort(external_sort()->memoryLimit(Unit::fromMb(16)));

        data_frame($config)
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->batchSize(1000)
            ->sortBy([ref('created_at')->desc()])
            ->run();
    }
}
