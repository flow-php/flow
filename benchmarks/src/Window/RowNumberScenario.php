<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Window;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\window;
use function Flow\Floe\DSL\from_floe;

final readonly class RowNumberScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->withEntry(
                'row_number',
                row_number()->over(window()->partitionBy(ref('seller_id'))->orderBy(ref('created_at'))),
            )
            ->run();
    }
}
