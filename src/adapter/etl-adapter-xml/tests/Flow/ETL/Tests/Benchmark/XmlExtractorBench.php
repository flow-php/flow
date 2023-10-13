<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Benchmark;

use Flow\ETL\DSL\XML;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(5)]
final class XmlExtractorBench
{
    #[Revs(1000)]
    public function bench_extract() : void
    {
        XML::from(__DIR__ . '/../Fixtures/simple_items.xml');
    }
}
