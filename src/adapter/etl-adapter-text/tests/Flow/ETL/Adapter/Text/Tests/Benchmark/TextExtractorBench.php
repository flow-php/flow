<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\DSL\Text;
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final class TextExtractorBench
{
    private readonly FlowContext $context;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
    }

    public function bench_extract_10k() : void
    {
        foreach (Text::from(__DIR__ . '/../Fixtures/orders_flow.csv')->extract($this->context) as $rows) {
        }
    }
}
