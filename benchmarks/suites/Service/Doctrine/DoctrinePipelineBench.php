<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

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
final class DoctrinePipelineBench
{
    public static function seed(): void
    {
        (new DoctrinePipelineScenario(BenchmarkRows::count()))->seed();
    }

    public static function drop(): void
    {
        (new DoctrinePipelineScenario(BenchmarkRows::count()))->dropTable();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['pipeline-service', 'service', 'service-doctrine'])]
    public function bench_pipeline_doctrine(array $params): void
    {
        (new DoctrinePipelineScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
