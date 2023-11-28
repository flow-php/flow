<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Benchmark;

use function Flow\ETL\DSL\from_avro;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final class AvroExtractorBench
{
    private readonly FlowContext $context;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
    }

    public function bench_extract_10k() : void
    {
        foreach (from_avro(__DIR__ . '/../Fixtures/orders_flow.avro')->extract($this->context) as $rows) {
        }
    }
}
