<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

/**
 * Resolves everything a source reads from — the fixture file or the in-memory array, and the first
 * DigestCache walk behind the fixture's name — so no timed window ever pays for it.
 */
final readonly class SourceFixture
{
    public function __construct(
        private Source $source,
        private int $rows,
    ) {}

    public function warm(): void
    {
        (new SourceExtractor($this->source, SchemaMode::declared, $this->rows))->extractor();
    }
}
