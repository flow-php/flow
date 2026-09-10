<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

final readonly class PipelineScenario
{
    public function __construct(
        private Source $source,
        private SchemaMode $mode,
        private int $rows,
    ) {}

    public function run(): void
    {
        (new CanonicalPipeline(
            (new SourceExtractor($this->source, $this->mode, $this->rows))->extractor(),
            $this->source->value,
        ))->run();
    }
}
