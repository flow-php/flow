<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Sorting;

use Generator;
use PhpBench\Attributes as Bench;

final class SortOrdersBench
{
    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['sorting'])]
    public function bench_sort_orders(array $params): void
    {
        (new SortOrdersScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
