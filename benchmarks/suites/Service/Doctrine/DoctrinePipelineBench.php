<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

/**
 * One bench class per service: BeforeClassMethods seeding runs regardless of group filtering.
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
