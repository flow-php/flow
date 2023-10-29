<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;

final class BufferExtractor implements Extractor, OverridingExtractor
{
    /**
     * @param int<1, max> $maxRowsSize
     */
    public function __construct(
        private readonly Extractor $extractor,
        private readonly int $maxRowsSize
    ) {
    }

    /**
     * @param FlowContext $context
     *
     * @return \Generator<int, Rows, mixed, void>
     */
    public function extract(FlowContext $context) : \Generator
    {
        $rows = new Rows();

        foreach ($this->extractor->extract($context) as $nextRows) {
            if ($nextRows->count() >= $this->maxRowsSize) {
                foreach ($nextRows->chunks($this->maxRowsSize) as $nextRowsChunk) {
                    if ($nextRowsChunk->count() === $this->maxRowsSize) {
                        yield $nextRowsChunk;
                    } else {
                        $rows = $rows->merge($nextRowsChunk);
                    }
                }
            } else {
                $rows = $rows->merge($nextRows);
            }

            if ($rows->count() >= $this->maxRowsSize) {
                $rowsChunks = $rows->chunks($this->maxRowsSize);
                $rows = new Rows();

                foreach ($rowsChunks as $rowsChunk) {
                    if ($rowsChunk->count() === $this->maxRowsSize) {
                        yield $rowsChunk;
                    } else {
                        $rows = $rowsChunk;
                    }
                }
            }
        }

        if ($rows->count()) {
            yield $rows;
        }
    }

    public function extractors() : array
    {
        return [$this->extractor];
    }
}
