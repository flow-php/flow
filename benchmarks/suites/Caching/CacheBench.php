<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Caching;

use Generator;
use PhpBench\Attributes as Bench;

final class CacheBench
{
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
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
