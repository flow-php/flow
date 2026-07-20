<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\Merge;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Row;
use Flow\ETL\Row\References;
use Generator;

final readonly class KWayMerge
{
    public function __construct(
        private BucketsStorage $storage,
        private References $refs,
    ) {}

    /**
     * @param list<string> $bucketIds
     *
     * @return Generator<Row>
     */
    public function merge(array $bucketIds): Generator
    {
        $heap = new RowsMinHeap(...$this->refs->all());

        /** @var array<string, Generator<Row>> $cursors */
        $cursors = [];

        foreach ($bucketIds as $id) {
            $cursor = $this->storage->get($id);

            if ($cursor->valid()) {
                $heap->push($cursor->current(), $id);
                $cursor->next();
                $cursors[$id] = $cursor;
            }
        }

        while (!$heap->isEmpty()) {
            $top = $heap->extract();

            yield $top->row;

            if (isset($cursors[$top->bucketId]) && $cursors[$top->bucketId]->valid()) {
                $heap->push($cursors[$top->bucketId]->current(), $top->bucketId);
                $cursors[$top->bucketId]->next();
            }
        }
    }
}
