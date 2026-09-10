<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\AfterClassMethods('dropWrite')]
#[Bench\BeforeMethods('warm')]
final class DoctrineWriteBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
    }

    public function setUpWrite(array $params): void
    {
        (new DoctrineWriteScenario((int) $params['rows']))->setUp();
    }

    public static function dropWrite(): void
    {
        (new DoctrineWriteScenario(100_000))->dropTable();

        (new DoctrineWriteScenario(BenchmarkRows::count()))->dropTable();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['service', 'service-doctrine'])]
    #[Bench\BeforeMethods('setUpWrite')]
    public function bench_doctrine_write(array $params): void
    {
        (new DoctrineWriteScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
