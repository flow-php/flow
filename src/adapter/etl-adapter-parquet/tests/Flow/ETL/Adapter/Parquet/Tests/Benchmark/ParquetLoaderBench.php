<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Benchmark;

use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\{Rows, Tests\Double\FakeStaticOrdersExtractor};
use PhpBench\Attributes\Groups;

#[Groups(['adapter-parquet'])]
final class ParquetLoaderBench
{
    private readonly string $outputPath;

    private Rows $rows;

    public function __construct()
    {
        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'etl_parquet_loader_bench') . '.parquet';
        $this->rows = (new FakeStaticOrdersExtractor(1_000))->toRows();
    }

    public function __destruct()
    {
        if (\file_exists($this->outputPath)) {
            \unlink($this->outputPath);
        }
    }

    public function bench_load_1k() : void
    {
        if (\file_exists($this->outputPath)) {
            \unlink($this->outputPath);
        }

        to_parquet($this->outputPath)->load($this->rows, flow_context(config()));
    }
}
