<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Benchmark;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\{FlowContext};
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final readonly class CSVExtractorBench
{
    private FlowContext $context;

    public function __construct()
    {
        $this->context = flow_context(config());
    }

    public function bench_extract_10k() : void
    {
        foreach (from_csv(__DIR__ . '/Fixtures/orders_flow.csv')->extract($this->context) as $rows) {
        }
    }
}
