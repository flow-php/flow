<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\{Extractor, FlowContext, Rows};

final readonly class BatchExtractor implements Extractor, OverridingExtractor
{
    /**
     * @param int<1, max> $chunkSize
     */
    public function __construct(
        private Extractor $extractor,
        private int $chunkSize,
    ) {
    }

    /**
     * @return \Generator<int, Rows, mixed, mixed>
     */
    public function extract(FlowContext $context) : \Generator
    {
        $chunk = new Rows();
        $chunkSize = 0;

        foreach ($this->extractor->extract($context) as $rows) {
            foreach ($rows->all() as $row) {
                $chunk = $chunk->add($row);
                $chunkSize++;

                if ($chunkSize === $this->chunkSize) {

                    $signal = yield $chunk;

                    if ($signal === Signal::STOP) {
                        return;
                    }
                    $chunkSize = 0;
                    $chunk = new Rows();
                }

                if ($chunkSize > $this->chunkSize) {

                    $signal = yield $chunk->dropRight($chunk->count() - $this->chunkSize);

                    if ($signal === Signal::STOP) {
                        return;
                    }
                    $chunk = $chunk->takeRight($chunk->count() - $this->chunkSize);
                    $chunkSize = $chunk->count();
                }
            }
        }

        if ($chunkSize) {
            yield $chunk;
        }
    }

    public function extractors() : array
    {
        return [$this->extractor];
    }
}
