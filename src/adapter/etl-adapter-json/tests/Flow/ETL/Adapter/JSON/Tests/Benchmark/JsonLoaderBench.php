<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Benchmark;

use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\{Rows, Tests\Double\FakeStaticOrdersExtractor};
use PhpBench\Attributes\Groups;

#[Groups(['loader'])]
final class JsonLoaderBench
{
    private readonly string $outputPath;

    private Rows $rows;

    public function __construct()
    {
        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'etl_json_loader_bench') . '.json';
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

        to_json($this->outputPath)->load($this->rows, flow_context(config()));
    }
}
