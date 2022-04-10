<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;

/**
 * @psalm-immutable
 */
final class ChunkExtractor implements Extractor
{
    public function __construct(
        private readonly Extractor $extractor,
        private readonly int $chunkSize
    ) {
    }

    public function extract() : \Generator
    {
        foreach ($this->extractor->extract() as $rows) {
            foreach ($rows->chunks($this->chunkSize) as $rowsChunk) {
                yield $rowsChunk;
            }
        }
    }
}
