<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\AfterClassMethods('dropWrappedWrite')]
#[Bench\BeforeMethods('warm')]
final class DoctrineWrappedWriteBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
    }

    public function setUpWrappedWrite(array $params): void
    {
        (new DoctrineWrappedWriteScenario((int) $params['rows']))->setUp();
    }

    public static function dropWrappedWrite(): void
    {
        (new DoctrineWrappedWriteScenario(100_000))->dropTable();

        (new DoctrineWrappedWriteScenario(BenchmarkRows::count()))->dropTable();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['service', 'service-doctrine'])]
    #[Bench\BeforeMethods('setUpWrappedWrite')]
    public function bench_doctrine_wrapped_write(array $params): void
    {
        (new DoctrineWrappedWriteScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
