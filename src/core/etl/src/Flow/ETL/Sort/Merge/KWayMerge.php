<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\Merge;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Generator;

use function count;

final readonly class KWayMerge
{
    public function __construct(
        private BucketsStorage $storage,
        private References $refs,
        private int $batchSize = 1000,
    ) {
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

        $batch = [];

        while (!$heap->isEmpty()) {
            $top = $heap->extract();
            $batch[] = $top->row;

            if (count($batch) >= $this->batchSize) {
                yield new Rows(...$batch);
                $batch = [];
            }

            $cursor = $cursors[$top->bucketId];

            if ($cursor->valid()) {
                $heap->push($cursor->current(), $top->bucketId);
                $cursor->next();
            }
        }

        if ($batch !== []) {
            yield new Rows(...$batch);
        }
    }
}
