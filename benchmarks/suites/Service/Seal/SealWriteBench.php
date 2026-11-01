<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Seal;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

// Disabled until an Elasticsearch 8.x service runs locally; the installed client cannot talk to ES 7.x.
#[Bench\Skip]
final class SealWriteBench
{
    public function setUpWrite(array $params): void
    {
        (new SealWriteScenario((int) $params['rows']))->setUp();
    }

    public static function dropWrite(): void
    {
        (new SealWriteScenario(100_000))->dropIndex();

        (new SealWriteScenario(BenchmarkRows::count()))->dropIndex();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['service', 'service-seal'])]
    #[Bench\BeforeMethods('setUpWrite')]
    public function bench_seal_write(array $params): void
    {
        (new SealWriteScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
