<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Filtering;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\Floe\DSL\from_floe;

final readonly class FilterOrdersScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame()
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->batchSize(1000)
            ->filter(ref('discount')->greaterThan(lit(25.0)))
            ->run();
    }
}
