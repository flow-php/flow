<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Grouping;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use function Flow\Floe\DSL\from_floe;

final readonly class GroupByAggregateScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->batchSize(1000)
            ->groupBy([ref('seller_id')])
            ->aggregate(count(ref('order_id')), sum(ref('discount')), average(ref('discount')), max(ref('discount')))
            ->run();
    }
}
