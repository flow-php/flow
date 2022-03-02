<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;

/**
 * @psalm-immutable
 */
final class ChunkExtractor implements Extractor
{
    private int $chunkSize;

    private Extractor $extractor;

    public function __construct(Extractor $extractor, int $chunkSize)
    {
        $this->extractor = $extractor;
        $this->chunkSize = $chunkSize;
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
