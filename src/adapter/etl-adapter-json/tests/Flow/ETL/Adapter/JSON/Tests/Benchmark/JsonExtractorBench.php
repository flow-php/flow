<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Benchmark;

use Flow\ETL\DSL\Json;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(5)]
final class JsonExtractorBench
{
    #[Revs(1000)]
    public function bench_extract() : void
    {
        Json::from(__DIR__ . '/../Fixtures/orders_flow.json');
    }
}
