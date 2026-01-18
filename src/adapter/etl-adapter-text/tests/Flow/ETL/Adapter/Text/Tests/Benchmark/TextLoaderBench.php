<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Benchmark;

use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\{Row, Rows, Tests\Double\FakeStaticOrdersExtractor};
use PhpBench\Attributes\Groups;

#[Groups(['loader'])]
final class TextLoaderBench
{
    private readonly string $outputPath;

    private Rows $rows;

    public function __construct()
    {
        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'etl_txt_loader_bench') . '.txt';
        $this->rows = (new FakeStaticOrdersExtractor(1_000))->toRows()->map(
            fn (Row $r) => \Flow\ETL\DSL\row($r->get('order_id'))
        );
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

        to_text($this->outputPath)->load($this->rows, flow_context(config()));
    }
}
