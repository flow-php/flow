<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Benchmark;

use Flow\ETL\DSL\Parquet;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(5)]
final class ParquetExtractorBench
{
    #[Revs(1000)]
    public function bench_extract() : void
    {
        Parquet::from(__DIR__ . '/../Fixtures/data.parquet');
    }
}
