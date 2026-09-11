<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Sorting;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\ref;
use function Flow\Floe\DSL\from_floe;

final readonly class SortOrdersScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        $config = BenchmarkConfig::builder()->sort(external_sort());

        data_frame($config)
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->batchSize(1000)
            ->sortBy([ref('created_at')->desc()])
            ->run();
    }
}
