<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Excel;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\DSL\data_frame;

final readonly class ExcelReadScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame()->read(from_excel(Datasets::orders($this->rows)->excel()))->run();
    }
}
