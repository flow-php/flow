<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;

final class SortBuckets
{
    /**
     * @param array<string, \Generator<Row>> $buckets
     */
    public function __construct(private readonly array $buckets)
    {
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
                $row = new CachedRow($bucket->current(), $bucketId);
                $heap->insert($row);
                $bucket->next();
            } else {
                unset($bucketsCopy[$bucketId]);
            }
        }

        while (!$heap->isEmpty()) {
            /** @var CachedRow $cachedRow */
            $cachedRow = $heap->extract();

            yield $cachedRow->row;

            if (isset($bucketsCopy[$cachedRow->generatorId])) {
                $bucket = $bucketsCopy[$cachedRow->generatorId];

                if ($bucket->valid()) {
                    $row = new CachedRow($bucket->current(), $cachedRow->generatorId);
                    $heap->insert($row);
                    $bucket->next();
                } else {
                    unset($bucketsCopy[$cachedRow->generatorId]);  // Remove the empty generator
                }
            }
        }
    }
}
