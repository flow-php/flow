<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Parquet;

use Flow\Benchmarks\Datasets\Datasets;
use Flow\Parquet\ParquetEngine;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\data_frame;

final readonly class ParquetReadScenario
{
    public function __construct(
        private int $rows,
        private ParquetEngine $engine,
    ) {}

    public function run(): void
    {
        data_frame()->read(from_parquet(Datasets::orders($this->rows)->parquet(), engine: $this->engine))->run();
    }
}
