<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Joining;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Flow\ETL\Join\Join;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeMethods('warm')]
final class JoinBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
        Datasets::sellers((int) $params['rows'])->parquet();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['joining'])]
    public function bench_join_inner(array $params): void
    {
        (new JoinOrdersWithSellersScenario(Join::inner, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['joining'])]
    public function bench_join_left(array $params): void
    {
        (new JoinOrdersWithSellersScenario(Join::left, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['joining'])]
    public function bench_join_left_anti(array $params): void
    {
        (new JoinOrdersWithSellersScenario(Join::left_anti, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['joining'])]
    public function bench_join_right(array $params): void
    {
        (new JoinOrdersWithSellersScenario(Join::right, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['joining'])]
    public function bench_cross_join(array $params): void
    {
        (new CrossJoinScenario((int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['joining'])]
    public function bench_join_each(array $params): void
    {
        (new JoinEachScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
