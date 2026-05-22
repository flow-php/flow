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
            $rows = $bucket->rows;

            if (is_array($rows)) {
                /** @var Iterator<Row> $iterator */
                $iterator = new ArrayIterator($rows);
            } elseif ($rows instanceof Iterator) {
                $iterator = $rows;
            } else {
                /** @var Iterator<Row> $iterator */
                $iterator = new IteratorIterator($rows);
            }

            $this->buckets[$bucket->id] = $iterator;
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
