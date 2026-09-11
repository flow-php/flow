<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Window;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Flow\Benchmarks\Partitioning\PartitionCardinality;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeMethods('warm')]
final class WindowBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
    }

    #[Bench\ParamProviders(['rows', 'cardinalities'])]
    #[Bench\Groups(['window'])]
    public function bench_moving_average(array $params): void
    {
        (new MovingAverageScenario(
            (int) $params['rows'],
            PartitionCardinality::from((string) $params['cardinality']),
        ))->run();
    }

    #[Bench\ParamProviders(['rows', 'cardinalities'])]
    #[Bench\Groups(['window'])]
    public function bench_rank(array $params): void
    {
        (new RankScenario((int) $params['rows'], PartitionCardinality::from((string) $params['cardinality'])))->run();
    }

    #[Bench\ParamProviders(['rows', 'cardinalities'])]
    #[Bench\Groups(['window'])]
    public function bench_row_number(array $params): void
    {
        (new RowNumberScenario(
            (int) $params['rows'],
            PartitionCardinality::from((string) $params['cardinality']),
        ))->run();
    }

    #[Bench\ParamProviders(['rows', 'cardinalities'])]
    #[Bench\Groups(['window'])]
    public function bench_running_total(array $params): void
    {
        (new RunningTotalScenario(
            (int) $params['rows'],
            PartitionCardinality::from((string) $params['cardinality']),
        ))->run();
    }

    #[Bench\ParamProviders(['rows', 'cardinalities'])]
    #[Bench\Groups(['window'])]
    public function bench_whole_partition(array $params): void
    {
        (new WholePartitionScenario(
            (int) $params['rows'],
            PartitionCardinality::from((string) $params['cardinality']),
        ))->run();
    }

    public function cardinalities(): Generator
    {
        foreach (PartitionCardinality::cases() as $cardinality) {
            yield $cardinality->value => ['cardinality' => $cardinality->value];
        }
    }

    /**
     * The orders dataset spreads rows over 5 sellers, so every row here lands in a partition of
     * rows/5. Frame-materializing aggregates are quadratic in partition size, hence a lower default
     * than the other suites - 100_000 rows would put bench_whole_partition into the minutes.
     */
    public function rows(): Generator
    {
        $rows = BenchmarkRows::windowCount();

        yield number_format($rows) => ['rows' => $rows];
    }
}
