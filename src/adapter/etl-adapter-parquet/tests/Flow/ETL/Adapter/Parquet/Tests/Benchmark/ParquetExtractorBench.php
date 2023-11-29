<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Benchmark;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final class ParquetExtractorBench
{
    private readonly FlowContext $context;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
    }

    public function bench_extract_10k() : void
    {
        foreach (from_parquet(__DIR__ . '/../Fixtures/orders_flow.parquet')->extract($this->context) as $rows) {
        }
    }
}
