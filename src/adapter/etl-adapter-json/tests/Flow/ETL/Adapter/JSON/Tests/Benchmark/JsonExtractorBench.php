<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;
use function Flow\ETL\Adapter\JSON\from_json;

#[Groups(['extractor'])]
final class JsonExtractorBench
{
    private readonly FlowContext $context;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
    }

    public function bench_extract_10k() : void
    {
        foreach (from_json(__DIR__ . '/../Fixtures/orders_flow.json')->extract($this->context) as $rows) {
        }
    }
}
