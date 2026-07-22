<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Caching;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_cache;
use function Flow\Floe\DSL\from_floe;

final readonly class CacheReadScenario
{
    private const CACHE_ID = 'bench_cache_read';

    public function __construct(
        private int $rows,
    ) {}

    public function warm(): void
    {
        data_frame(BenchmarkConfig::builder())
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->batchSize(1000)
            ->cache(self::CACHE_ID)
            ->run();
    }

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())->read(from_cache(self::CACHE_ID))->batchSize(1000)->run();
    }
}
