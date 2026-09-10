<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Postgresql;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\AfterClassMethods('dropWrite')]
#[Bench\BeforeMethods('warm')]
final class PostgresqlWriteBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
    }

    public function setUpWrite(array $params): void
    {
        (new PostgresqlWriteScenario((int) $params['rows']))->setUp();
    }

    public static function dropWrite(): void
    {
        (new PostgresqlWriteScenario(100_000))->dropTable();

        (new PostgresqlWriteScenario(BenchmarkRows::count()))->dropTable();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['service', 'service-postgresql'])]
    #[Bench\BeforeMethods('setUpWrite')]
    public function bench_postgresql_write(array $params): void
    {
        (new PostgresqlWriteScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
