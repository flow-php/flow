<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Joining;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\Floe\DSL\from_floe;

final readonly class CrossJoinScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->batchSize(1000)
            ->crossJoin(data_frame()->read(from_array([['factor' => 1], ['factor' => 2]])), 'cross_')
            ->run();
    }
}
