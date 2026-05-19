<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

use ArrayIterator;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Generator;
use Iterator;
use IteratorIterator;

use function array_keys;
use function is_array;

final class Buckets
{
    /**
     * @var array<string, \Iterator<Row>>
     */
    private array $buckets = [];

    /**
     * @param array<Bucket> $buckets
     */
    public function __construct(array $buckets)
    {
        foreach ($buckets as $bucket) {
            if (is_array($bucket->rows)) {
                // @mago-ignore analysis:mixed-property-type-coercion
                $this->buckets[$bucket->id] = new ArrayIterator($bucket->rows);
            } elseif ($bucket->rows instanceof Iterator) {
                // @mago-ignore analysis:mixed-property-type-coercion
                $this->buckets[$bucket->id] = $bucket->rows;
            } else {
                // @mago-ignore analysis:mixed-property-type-coercion
                $this->buckets[$bucket->id] = new IteratorIterator($bucket->rows);
            }
        }
    }

    /**
     * @return array<string>
     */
    public function bucketIds(): array
    {
        return array_keys($this->buckets);
    }

    /**
     * @return \Generator<Row>
     */
    public function sort(Reference ...$refs): Generator
    {
        $heap = new RowsMinHeap(...$refs);

        $bucketsCopy = $this->buckets;

        foreach ($bucketsCopy as $bucketId => $bucket) {
            if ($bucket->valid()) {
                // @mago-ignore analysis:possibly-null-argument
                $row = new BucketRow($bucket->current(), $bucketId);
                $heap->insert($row);
                $bucket->next();
            } else {
                unset($bucketsCopy[$bucketId]);
            }
        }

        while (!$heap->isEmpty()) {
            // @mago-ignore analysis:redundant-docblock-type
            /** @var BucketRow $cachedRow */
            $cachedRow = $heap->extract();

            yield $cachedRow->row;

            if (isset($bucketsCopy[$cachedRow->bucketId])) {
                $bucket = $bucketsCopy[$cachedRow->bucketId];

                if ($bucket->valid()) {
                    // @mago-ignore analysis:possibly-null-argument
                    $row = new BucketRow($bucket->current(), $cachedRow->bucketId);
                    $heap->insert($row);
                    $bucket->next();
                } else {
                    unset($bucketsCopy[$cachedRow->bucketId]); // Remove the empty generator
                }
            }
        }
    }
}
