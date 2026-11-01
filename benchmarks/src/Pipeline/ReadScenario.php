<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\ETL\IO\Extractor;

use function Flow\ETL\DSL\data_frame;

/**
 * Rung 1: same config and declared schema as higher rungs, or its number is not subtractable.
 */
final readonly class ReadScenario
{
    public function __construct(
        private Source $source,
        private int $rows,
    ) {}

    public function extractor(): Extractor
    {
        return (new SourceExtractor($this->source, SchemaMode::declared, $this->rows))->extractor();
    }

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())->read($this->extractor())->run();
    }
}
