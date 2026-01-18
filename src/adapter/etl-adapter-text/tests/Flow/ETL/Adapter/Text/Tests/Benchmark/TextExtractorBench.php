<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Benchmark;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final readonly class TextExtractorBench
{
    private FlowContext $context;

    public function __construct()
    {
        $this->context = flow_context(config());
    }

    public function bench_extract_1k() : void
    {
        foreach (from_text(__DIR__ . '/../Fixtures/orders_1k.csv')->extract($this->context) as $rows) {
        }
    }
}
