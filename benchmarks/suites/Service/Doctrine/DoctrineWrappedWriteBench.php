<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

use Generator;
use PhpBench\Attributes as Bench;

#[Bench\AfterClassMethods('dropWrappedWrite')]
final class DoctrineWrappedWriteBench
{
    public function setUpWrappedWrite(array $params): void
    {
        (new DoctrineWrappedWriteScenario((int) $params['rows']))->setUp();
    }

    public static function dropWrappedWrite(): void
    {
        (new DoctrineWrappedWriteScenario(100_000))->dropTable();

        (new DoctrineWrappedWriteScenario((int) (getenv('FLOW_BENCH_ROWS') ?: 100_000)))->dropTable();
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
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
