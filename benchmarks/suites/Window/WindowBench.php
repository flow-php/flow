<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Window;

use Generator;
use PhpBench\Attributes as Bench;

final class WindowBench
{
    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['window'])]
    public function bench_moving_average(array $params): void
    {
        (new MovingAverageScenario((int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['window'])]
    public function bench_rank(array $params): void
    {
        (new RankScenario((int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['window'])]
    public function bench_row_number(array $params): void
    {
        (new RowNumberScenario((int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['window'])]
    public function bench_running_total(array $params): void
    {
        (new RunningTotalScenario((int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['window'])]
    public function bench_whole_partition(array $params): void
    {
        (new WholePartitionScenario((int) $params['rows']))->run();
    }

    /**
     * The orders dataset spreads rows over 5 sellers, so every row here lands in a partition of
     * rows/5. Frame-materializing aggregates are quadratic in partition size, hence a lower default
     * than the other suites - 100_000 rows would put bench_whole_partition into the minutes.
     */
    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_WINDOW_ROWS') ?: 10_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
