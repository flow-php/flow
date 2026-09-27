<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Floe;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

final readonly class FloeReadScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame()->read(from_floe(Datasets::orders($this->rows)->floe()))->run();
    }
}
