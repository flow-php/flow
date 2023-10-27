<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;

final class ChunkExtractor implements Extractor, OverridingExtractor
{
    /**
     * @param int<1, max> $chunkSize
     */
    public function __construct(
        private readonly Extractor $extractor,
        private readonly int $chunkSize
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->extractor->extract($context) as $rows) {
            foreach ($rows->chunks($this->chunkSize) as $rowsChunk) {
                yield $rowsChunk;
            }
        }
    }

    public function extractors() : array
    {
        return [$this->extractor];
    }
}
