<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Postgresql;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeClassMethods('seedRead')]
#[Bench\AfterClassMethods('dropRead')]
final class PostgresqlReadBench
{
    public static function seedRead(): void
    {
        (new PostgresqlReadScenario(BenchmarkRows::count()))->seed();
    }

    public static function dropRead(): void
    {
        (new PostgresqlReadScenario(BenchmarkRows::count()))->dropTable();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['service', 'service-postgresql'])]
    public function bench_postgresql_read(array $params): void
    {
        (new PostgresqlReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
