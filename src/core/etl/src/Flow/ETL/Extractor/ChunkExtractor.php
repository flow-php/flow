<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\{Extractor, FlowContext, Rows};

final class ChunkExtractor implements Extractor, OverridingExtractor
{
    public function __construct(
        private readonly Extractor $extractor,
        private readonly int $chunkSize,
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $chunk = [];
        $chunkSize = 0;

        foreach ($this->extractor->extract($context) as $rows) {
            foreach ($rows->chunks($this->chunkSize) as $rowsChunk) {
                $chunk[] = $rowsChunk->all();
                $chunkSize += $rowsChunk->count();

                if ($chunkSize === $this->chunkSize) {
                    $signal = yield new Rows(
                        ...\array_merge(
                            ...$chunk
                        )
                    );

                    if ($signal === Signal::STOP) {
                        return;
                    }
                    $chunkSize = 0;
                    $chunk = [];
                }

                if ($chunkSize > $this->chunkSize) {
                    $allRows = new Rows(
                        ...\array_merge(
                            ...$chunk
                        )
                    );

                    $signal = yield $allRows->dropRight($allRows->count() - $this->chunkSize);

                    if ($signal === Signal::STOP) {
                        return;
                    }
                    $leftover = $allRows->takeRight($allRows->count() - $this->chunkSize);
                    $chunk = [$leftover->all()];
                    $chunkSize = $leftover->count();
                }
            }
        }

        if ($chunkSize) {
            yield new Rows(
                ...\array_merge(
                    ...$chunk
                )
            );
        }
    }

    public function extractors() : array
    {
        return [$this->extractor];
    }
}
