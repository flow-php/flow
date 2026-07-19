<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeClassMethods('seedRead')]
#[Bench\AfterClassMethods('dropRead')]
final class DoctrineReadBench
{
    public static function seedRead(): void
    {
        (new DoctrineReadScenario((int) (getenv('FLOW_BENCH_ROWS') ?: 100_000)))->seed();
    }

    public static function dropRead(): void
    {
        (new DoctrineReadScenario((int) (getenv('FLOW_BENCH_ROWS') ?: 100_000)))->dropTable();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['service', 'service-doctrine'])]
    public function bench_doctrine_read(array $params): void
    {
        (new DoctrineReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
