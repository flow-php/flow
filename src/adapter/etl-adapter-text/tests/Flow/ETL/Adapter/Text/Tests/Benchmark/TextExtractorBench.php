<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Benchmark;

use function Flow\ETL\Adapter\Text\from_text;
use Flow\ETL\{Config, FlowContext};
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final readonly class TextExtractorBench
{
    private FlowContext $context;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
    }

    public function bench_extract_10k() : void
    {
        foreach (from_text(__DIR__ . '/../Fixtures/orders_flow.csv')->extract($this->context) as $rows) {
        }
    }
}
