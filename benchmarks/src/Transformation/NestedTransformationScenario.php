<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Transformation;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Datasets;
use Flow\ETL\Loader;

use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

final readonly class NestedTransformationScenario
{
    public function __construct(
        private int $rows,
        private Loader $loader,
    ) {}

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->batchSize(1000)
            ->write($this->loader)
            ->run();
    }
}
