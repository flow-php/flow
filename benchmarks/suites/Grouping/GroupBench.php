<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Grouping;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeMethods('warm')]
final class GroupBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['grouping'])]
    public function bench_group_by(array $params): void
    {
        (new GroupByScenario((int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['grouping'])]
    public function bench_group_by_aggregate(array $params): void
    {
        (new GroupByAggregateScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
