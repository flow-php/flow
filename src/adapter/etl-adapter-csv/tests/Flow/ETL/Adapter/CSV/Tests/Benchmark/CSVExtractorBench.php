<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Benchmark;

use Flow\ETL\DSL\CSV;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(5)]
#[Groups(['extractor'])]
final class CSVExtractorBench
{
    #[Revs(1000)]
    public function bench_extract() : void
    {
        CSV::from(__DIR__ . '/../Fixtures/orders_flow.csv');
    }
}
