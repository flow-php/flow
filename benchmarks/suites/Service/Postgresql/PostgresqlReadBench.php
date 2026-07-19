<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Postgresql;

use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeClassMethods('seedRead')]
#[Bench\AfterClassMethods('dropRead')]
final class PostgresqlReadBench
{
    public static function seedRead(): void
    {
        (new PostgresqlReadScenario((int) (getenv('FLOW_BENCH_ROWS') ?: 100_000)))->seed();
    }

    public static function dropRead(): void
    {
        (new PostgresqlReadScenario((int) (getenv('FLOW_BENCH_ROWS') ?: 100_000)))->dropTable();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['service', 'service-postgresql'])]
    public function bench_postgresql_read(array $params): void
    {
        (new PostgresqlReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
