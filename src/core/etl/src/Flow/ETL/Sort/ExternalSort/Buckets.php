<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;

final class Buckets
{
    /**
     * @param array<string, Bucket> $buckets
     */
    private array $buckets = [];

    /**
     * @param array<Bucket> $buckets
     */
    public function __construct(array $buckets)
    {
        foreach ($buckets as $bucket) {
            $this->buckets[$bucket->id] = $bucket->rows;
        }
    }

    /**
     * @return array<string>
     */
    public function bucketIds() : array
    {
        return \array_keys($this->buckets);
    }

    /**
     * @return \Generator<Row>
     */
    public function sort(Reference ...$refs) : \Generator
    {
        $heap = new RowsMinHeap(...$refs);

        $bucketsCopy = $this->buckets;

        foreach ($bucketsCopy as $bucketId => $bucket) {
            if ($bucket->valid()) {
                $row = new BucketRow($bucket->current(), $bucketId);
                $heap->insert($row);
                $bucket->next();
            } else {
                unset($bucketsCopy[$bucketId]);
            }
        }

        while (!$heap->isEmpty()) {
            /** @var BucketRow $cachedRow */
            $cachedRow = $heap->extract();

            yield $cachedRow->row;

            if (isset($bucketsCopy[$cachedRow->bucketId])) {
                $bucket = $bucketsCopy[$cachedRow->bucketId];

                if ($bucket->valid()) {
                    $row = new BucketRow($bucket->current(), $cachedRow->bucketId);
                    $heap->insert($row);
                    $bucket->next();
                } else {
                    unset($bucketsCopy[$cachedRow->bucketId]);  // Remove the empty generator
                }
            }
        }
    }
}
