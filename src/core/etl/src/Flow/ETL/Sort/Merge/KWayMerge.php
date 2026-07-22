<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\Merge;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\References;
use Flow\ETL\Row\RowsBuffer;
use Flow\ETL\Rows;
use Generator;

final readonly class KWayMerge
{
    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private BucketsStorage $storage,
        private References $refs,
        private int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    /**
     * @param list<string> $bucketIds
     *
     * @return Generator<Rows>
     */
    public function merge(array $bucketIds): Generator
    {
        $heap = new RowsMinHeap(...$this->refs->all());

        /** @var array<string, BucketCursor> $cursors */
        $cursors = [];

        foreach ($bucketIds as $id) {
            $cursor = new BucketCursor($this->storage->get($id));

            if ($cursor->valid()) {
                $heap->push($cursor->current(), $id);
                $cursor->next();
                $cursors[$id] = $cursor;
            }
        }

        $buffer = new RowsBuffer($this->batchSize);

        while (!$heap->isEmpty()) {
            $top = $heap->extract();

            if (null !== ($batch = $buffer->add($top->row))) {
                yield $batch;
            }

            $cursor = $cursors[$top->bucketId];

            if ($cursor->valid()) {
                $heap->push($cursor->current(), $top->bucketId);
                $cursor->next();
            }
        }

        if (null !== ($batch = $buffer->flush())) {
            yield $batch;
        }
    }
}
