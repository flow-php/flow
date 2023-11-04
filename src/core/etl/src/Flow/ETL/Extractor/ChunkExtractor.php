<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;

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
        $chunk = new Rows();

        foreach ($this->extractor->extract($context) as $rows) {
            foreach ($rows->chunks($this->chunkSize) as $rowsChunk) {
                $chunk = $chunk->merge($rowsChunk);

                if ($chunk->count() === $this->chunkSize) {
                    $signal = yield $chunk;

                    if ($signal === Signal::STOP) {
                        return;
                    }
                    $chunk = new Rows();
                }

                if ($chunk->count() > $this->chunkSize) {
                    $signal = yield $chunk->dropRight($chunk->count() - $this->chunkSize);

                    if ($signal === Signal::STOP) {
                        return;
                    }
                    $chunk = $chunk->takeRight($chunk->count() - $this->chunkSize);
                }
            }
        }

        if ($chunk->count()) {
            yield $chunk;
        }
    }

    public function extractors() : array
    {
        return [$this->extractor];
    }
}
