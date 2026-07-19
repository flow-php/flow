<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Csv;

use Flow\Benchmarks\Datasets\Datasets;
use Flow\Benchmarks\Datasets\Paths;

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;
use function uniqid;

final readonly class CsvWriteScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame()
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->write(to_csv(Paths::var() . '/format_write_csv_' . uniqid('', true) . '.csv'))
            ->run();
    }
}
