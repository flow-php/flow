<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Postgresql;

use Generator;
use PhpBench\Attributes as Bench;

#[Bench\AfterClassMethods('dropWrite')]
final class PostgresqlWriteBench
{
    public function setUpWrite(array $params): void
    {
        (new PostgresqlWriteScenario((int) $params['rows']))->setUp();
    }

    public static function dropWrite(): void
    {
        (new PostgresqlWriteScenario(100_000))->dropTable();

        (new PostgresqlWriteScenario((int) (getenv('FLOW_BENCH_ROWS') ?: 100_000)))->dropTable();
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
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
