<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\DSL\XML;
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(3)]
#[Groups(['extractor'])]
final class XmlExtractorBench
{
    #[Revs(5)]
    public function bench_extract() : void
    {
        foreach (XML::from(__DIR__ . '/../Fixtures/simple_items.xml')->extract(new FlowContext(Config::default())) as $rows) {
        }
    }
}
