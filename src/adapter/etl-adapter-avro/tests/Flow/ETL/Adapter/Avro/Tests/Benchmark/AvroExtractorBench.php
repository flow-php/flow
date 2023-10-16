<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Benchmark;

use Flow\ETL\DSL\Avro;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(5)]
final class AvroExtractorBench
{
    #[Revs(1000)]
    public function bench_extract() : void
    {
        Avro::from(__DIR__ . '/../Fixtures/data.avro');
    }
}
