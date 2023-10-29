<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\DSL\Text;
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(3)]
#[Groups(['extractor'])]
final class TextExtractorBench
{
    private FlowContext $context;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
    }

    #[Revs(5)]
    public function bench_extract_10k() : void
    {
        foreach (Text::from(__DIR__ . '/../Fixtures/orders_flow.csv')->extract($this->context) as $rows) {
        }
    }
}
