<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeClassMethods('seedRead')]
#[Bench\AfterClassMethods('dropRead')]
final class DoctrineReadBench
{
    public static function seedRead(): void
    {
        (new DoctrineReadScenario(BenchmarkRows::count()))->seed();
    }

    public static function dropRead(): void
    {
        (new DoctrineReadScenario(BenchmarkRows::count()))->dropTable();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['service', 'service-doctrine'])]
    public function bench_doctrine_read(array $params): void
    {
        (new DoctrineReadScenario((int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'limits'])]
    #[Bench\Groups(['service', 'service-doctrine'])]
    public function bench_limit_pushdown(array $params): void
    {
        (new DoctrineReadScenario(
            (int) $params['rows'],
            $params['limit'] === null ? null : (int) $params['limit'],
        ))->run();
    }

    public function limits(): Generator
    {
        yield 'none' => ['limit' => null];

        yield '1000' => ['limit' => 1000];
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
