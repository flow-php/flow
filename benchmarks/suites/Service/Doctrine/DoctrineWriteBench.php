<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

use Generator;
use PhpBench\Attributes as Bench;

#[Bench\AfterClassMethods('dropWrite')]
final class DoctrineWriteBench
{
    public function setUpWrite(array $params): void
    {
        (new DoctrineWriteScenario((int) $params['rows']))->setUp();
    }

    public static function dropWrite(): void
    {
        (new DoctrineWriteScenario(100_000))->dropTable();

        (new DoctrineWriteScenario((int) (getenv('FLOW_BENCH_ROWS') ?: 100_000)))->dropTable();
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
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
