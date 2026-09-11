<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Seal;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

// Disabled until an Elasticsearch 8.x service is available locally (the installed client is 8.x and cannot talk to
// ES 7.x). To re-enable once compose.yml's elasticsearch:8.19.0 container is up: remove #[Bench\Skip] and restore the
// class-level #[Bench\AfterClassMethods('dropWrite')] teardown (dropped here so the disabled bench never contacts ES).
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
