<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\DSL\Avro;
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(3)]
#[Groups(['extractor'])]
final class AvroExtractorBench
{
    private FlowContext $context;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
    }

    #[Revs(5)]
    public function bench_extract_10k() : void
    {
        foreach (Avro::from(__DIR__ . '/../Fixtures/orders_flow.avro')->extract($this->context) as $rows) {
        }
    }
}
