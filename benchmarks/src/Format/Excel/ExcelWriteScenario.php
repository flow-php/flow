<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Excel;

use Flow\Benchmarks\Datasets\Datasets;
use Flow\Benchmarks\Datasets\Paths;

use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;
use function uniqid;

final readonly class ExcelWriteScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame()
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->write(to_excel(Paths::var() . '/format_write_excel_' . uniqid('', true) . '.xlsx'))
            ->run();
    }
}
