<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Postgresql;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

/**
 * One bench class per service: BeforeClassMethods seeding runs regardless of group filtering.
 */
#[Bench\BeforeClassMethods('seed')]
#[Bench\AfterClassMethods('drop')]
final class PostgresqlPipelineBench
{
    public static function seed(): void
    {
        (new PostgresqlPipelineScenario(BenchmarkRows::count()))->seed();
    }

    public static function drop(): void
    {
        (new PostgresqlPipelineScenario(BenchmarkRows::count()))->dropTable();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['pipeline-service', 'service', 'service-postgresql'])]
    public function bench_pipeline_postgresql(array $params): void
    {
        (new PostgresqlPipelineScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
