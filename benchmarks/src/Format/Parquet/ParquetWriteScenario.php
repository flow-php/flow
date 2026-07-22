<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Parquet;

use Flow\Benchmarks\Datasets\Datasets;
use Flow\Benchmarks\Datasets\Paths;
use Flow\Parquet\ParquetEngine;

use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;
use function uniqid;

final readonly class ParquetWriteScenario
{
    public function __construct(
        private int $rows,
        private ParquetEngine $engine,
    ) {}

    public function run(): void
    {
        data_frame()
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->write(to_parquet(
                Paths::var() . '/format_write_parquet_' . uniqid('', true) . '.parquet',
                engine: $this->engine,
            ))
            ->run();
    }
}
