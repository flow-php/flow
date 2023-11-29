<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Benchmark;

use function Flow\ETL\Adapter\CSV\from_csv;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final class CSVExtractorBench
{
    private readonly FlowContext $context;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
    }

    public function bench_extract_10k() : void
    {
        foreach (from_csv(__DIR__ . '/../Fixtures/orders_flow.csv')->extract($this->context) as $rows) {
        }
    }
}
