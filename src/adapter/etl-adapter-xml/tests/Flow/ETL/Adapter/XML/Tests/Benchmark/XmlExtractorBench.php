<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Benchmark;

use Flow\ETL\DSL\XML;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(5)]
#[Groups(['extractor'])]
final class XmlExtractorBench
{
    #[Revs(1000)]
    public function bench_extract() : void
    {
        XML::from(__DIR__ . '/../Fixtures/simple_items.xml');
    }
}
