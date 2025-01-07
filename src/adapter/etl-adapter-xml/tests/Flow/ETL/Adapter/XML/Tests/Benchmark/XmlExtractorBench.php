<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Benchmark;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\{FlowContext};
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final readonly class XmlExtractorBench
{
    private FlowContext $context;

    public function __construct()
    {
        $this->context = flow_context(config());
    }

    public function bench_extract_10k() : void
    {
        foreach (from_xml(__DIR__ . '/../Fixtures/flow_orders.xml', xml_node_path: 'root/row')->extract($this->context) as $rows) {
        }
    }
}
