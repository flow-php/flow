<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Benchmark;

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\{FlowContext};
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final readonly class ExcelExtractorBench
{
    private FlowContext $context;

    public function __construct()
    {
        $this->context = flow_context(config());
    }

    public function bench_extract_10k() : void
    {
        iterator_to_array(
            from_excel(__DIR__ . '/../Fixtures/orders_flow.xlsx')->extract($this->context)
        );
    }
}
