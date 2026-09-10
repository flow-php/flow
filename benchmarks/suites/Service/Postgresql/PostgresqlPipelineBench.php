<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Postgresql;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

/**
 * Grouped pipeline-service and service, deliberately not pipeline, so --group=pipeline runs anywhere
 * and the service pipelines are opted into.
 *
 * Seeding crosses the process boundary through the database, so it belongs in BeforeClassMethods.
 * phpbench runs class hooks once per class regardless of group filtering, which is why each service
 * has its own bench class - a shared one would seed the other service's table for a single subject.
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
