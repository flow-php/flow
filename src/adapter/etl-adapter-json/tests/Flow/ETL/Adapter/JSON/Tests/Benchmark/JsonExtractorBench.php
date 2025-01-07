<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Benchmark;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\{FlowContext};
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final readonly class JsonExtractorBench
{
    private FlowContext $context;

    public function __construct()
    {
        $this->context = flow_context(config());
    }

    public function bench_extract_10k() : void
    {
        foreach (from_json(__DIR__ . '/../Fixtures/orders_flow.json')->extract($this->context) as $rows) {
        }
    }
}
