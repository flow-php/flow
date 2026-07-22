<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Grouping;

use Generator;
use PhpBench\Attributes as Bench;

final class GroupBench
{
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
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
