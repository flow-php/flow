<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Csv;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\data_frame;

final readonly class CsvReadScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame()->read(from_csv(Datasets::orders($this->rows)->csv()))->run();
    }
}
