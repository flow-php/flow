<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\DSL\Json;
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(3)]
#[Groups(['extractor'])]
final class JsonExtractorBench
{
    #[Revs(5)]
    public function bench_extract_10k() : void
    {
        foreach (Json::from(__DIR__ . '/../Fixtures/orders_flow.json')->extract(new FlowContext(Config::default())) as $rows) {
        }
    }
}
