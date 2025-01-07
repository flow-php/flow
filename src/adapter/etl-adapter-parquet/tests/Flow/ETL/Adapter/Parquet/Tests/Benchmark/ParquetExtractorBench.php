<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Benchmark;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\{FlowContext};
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final readonly class ParquetExtractorBench
{
    private FlowContext $context;

    public function __construct()
    {
        $this->context = flow_context(config());
    }

    public function bench_extract_10k() : void
    {
        foreach (from_parquet(__DIR__ . '/Fixtures/orders_10k.parquet')->extract($this->context) as $rows) {
        }
    }
}
