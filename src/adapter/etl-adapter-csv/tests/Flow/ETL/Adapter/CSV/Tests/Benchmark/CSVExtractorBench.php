<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Benchmark;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;

#[Groups(['adapter-csv'])]
final readonly class CSVExtractorBench
{
    private FlowContext $context;

    public function __construct()
    {
        $this->context = flow_context(config());
    }

    public function bench_extract_1k() : void
    {
        foreach (from_csv(__DIR__ . '/Fixtures/orders_1k.csv')->extract($this->context) as $rows) {
        }
    }
}
