<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Caching;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeMethods('warm')]
final class CacheBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['caching'])]
    public function bench_cache_write(array $params): void
    {
        (new CacheWriteScenario((int) $params['rows']))->run();
    }

    public function warmCacheRead(array $params): void
    {
        (new CacheReadScenario((int) $params['rows']))->warm();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['caching'])]
    #[Bench\BeforeMethods('warmCacheRead')]
    public function bench_cache_read(array $params): void
    {
        (new CacheReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
