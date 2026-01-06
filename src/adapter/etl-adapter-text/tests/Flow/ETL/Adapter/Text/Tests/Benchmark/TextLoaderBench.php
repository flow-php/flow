<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Benchmark;

use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\{FlowContext, Row, Rows, Tests\Double\FakeStaticOrdersExtractor};
use PhpBench\Attributes\Groups;

#[Groups(['loader'])]
final class TextLoaderBench
{
    private readonly FlowContext $context;

    private readonly string $outputPath;

    private Rows $rows;

    public function __construct()
    {
        $this->context = flow_context(config());
        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'etl_txt_loader_bench') . '.txt';
        $this->rows = (new FakeStaticOrdersExtractor(10_000))->toRows()->map(
            fn (Row $r) => \Flow\ETL\DSL\row($r->get('order_id'))
        );
    }

    public function __destruct()
    {
        if (!\file_exists($this->outputPath)) {
            throw new \RuntimeException("Benchmark failed, \"{$this->outputPath}\" doesn't exist");
        }

        \unlink($this->outputPath);
    }

    public function bench_load_10k() : void
    {
        to_text($this->outputPath)->load($this->rows, $this->context);
    }
}
