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
    private FlowContext $context;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
    }

    #[Revs(5)]
    public function bench_extract_10k() : void
    {
        foreach (XML::from(__DIR__ . '/../Fixtures/flow_orders.xml', xml_node_path: 'root/row')->extract($this->context) as $rows) {
        }
    }
}
