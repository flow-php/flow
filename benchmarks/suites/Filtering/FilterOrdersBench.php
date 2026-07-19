<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Filtering;

use Generator;
use PhpBench\Attributes as Bench;

final class FilterOrdersBench
{
    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['filtering'])]
    public function bench_filter_orders(array $params): void
    {
        (new FilterOrdersScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
