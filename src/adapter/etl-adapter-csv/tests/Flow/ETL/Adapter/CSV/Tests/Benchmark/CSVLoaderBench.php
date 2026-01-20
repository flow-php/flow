<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Benchmark;

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\{Rows, Tests\Double\FakeStaticOrdersExtractor};
use PhpBench\Attributes\Groups;

#[Groups(['adapter-csv'])]
final class CSVLoaderBench
{
    private readonly string $outputPath;

    private Rows $rows;

    public function __construct()
    {
        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'etl_csv_loader_bench') . '.csv';
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

        to_csv($this->outputPath)->load($this->rows, flow_context(config()));
    }
}
