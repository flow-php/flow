<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\DSL\Text;
use Flow\ETL\FlowContext;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(3)]
#[Groups(['extractor'])]
final class TextExtractorBench
{
    #[Revs(5)]
    public function bench_extract() : void
    {
        foreach (Text::from(__DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv')->extract(new FlowContext(Config::default())) as $rows) {
        }
    }
}
