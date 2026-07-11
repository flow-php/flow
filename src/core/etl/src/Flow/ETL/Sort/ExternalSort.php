<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Sort\ExternalSort\Bucket;
use Flow\ETL\Sort\ExternalSort\Buckets;
use Flow\ETL\Sort\ExternalSort\BucketsCache;
use Generator;

use function abs;
use function array_chunk;
use function bin2hex;
use function count;
use function iterator_to_array;
use function max;
use function random_bytes;

/**
 * External sorting is explained here:.
 *
 * https://medium.com/outco/how-to-merge-k-sorted-arrays-c35d87aa298e
 * https://web.archive.org/web/20150202022830/http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
 */
final class ExternalSort implements SortingAlgorithm
{
    private int $batchSize = -1;

    /**
     * @param BucketsCache $bucketsCache
     * @param int<1,max> $bucketsCount - how many buckets are merged at once; higher number reduces IO but increases memory consumption
     * @param int<1,max> $bucketSize - rows buffered and sorted in memory before they are spilled as one bucket
     */
    public function __construct(
        private readonly BucketsCache $bucketsCache,
        private readonly int $bucketsCount = 10,
        private readonly int $bucketSize = 10_000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0, given: ' . $this->bucketsCount);
        }

        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->bucketSize < 1) {
            throw new InvalidArgumentException('Bucket size must be greater than 0, given: ' . $this->bucketSize);
        }
    }

    /**
     * @return \Generator<Rows>
     */
    public function sortGenerator(Generator $rows, FlowContext $context, References $refs): Generator
    {
        $buckets = iterator_to_array($this->createBucketsFromGenerator($rows, $refs));

        while (count($buckets) > $this->bucketsCount) {
            $merged = [];

            foreach (array_chunk($buckets, $this->bucketsCount) as $bucketsChunk) {
                $merged[] = $this->mergeToBucket(new Buckets($bucketsChunk), $refs);
            }

            $buckets = $merged;
        }

        yield from $this->extractSortedRows(new Buckets($buckets), $refs);
    }

    /**
     * @param array<Row> $buffer
     */
    private function createBucket(array $buffer, References $refs): Bucket
    {
        $bucketId = bin2hex(random_bytes(16));
        $this->bucketsCache->set($bucketId, (new Rows(...$buffer))->sortBy(...$refs));

        return new Bucket($bucketId, $this->bucketsCache->get($bucketId));
    }

    /**
     * @param \Generator<Rows> $generator
     *
     * @return \Generator<Bucket>
     */
    private function createBucketsFromGenerator(Generator $generator, References $refs): Generator
    {
        /** @var array<Row> $buffer */
        $buffer = [];

        foreach ($generator as $batch) {
            if ($this->batchSize === -1) {
                $this->batchSize = $batch->count();
            }

            foreach ($batch as $row) {
                $buffer[] = $row;
            }

            if (count($buffer) >= $this->bucketSize) {
                yield $this->createBucket($buffer, $refs);
                $buffer = [];
            }
        }

        if ($buffer !== []) {
            yield $this->createBucket($buffer, $refs);
        }
    }

    /**
     * Final merge streams rows straight from the remaining buckets instead of writing the fully
     * sorted dataset back to the cache and reading it again.
     *
     * @return \Generator<Rows>
     */
    private function extractSortedRows(Buckets $buckets, References $refs): Generator
    {
        $outputBatchSize = max(1, abs($this->batchSize));

        /** @var array<Row> $buffer */
        $buffer = [];

        foreach ($buckets->sort(...$refs->all()) as $row) {
            $buffer[] = $row;

            if (count($buffer) >= $outputBatchSize) {
                yield new Rows(...$buffer);
                $buffer = [];
            }
        }

        if ($buffer !== []) {
            yield new Rows(...$buffer);
        }

        foreach ($buckets->bucketIds() as $bucketId) {
            $this->bucketsCache->remove($bucketId);
        }
    }

    private function mergeToBucket(Buckets $buckets, References $refs): Bucket
    {
        $this->bucketsCache->set($nextBucketId = bin2hex(random_bytes(16)), $buckets->sort(...$refs->all()));

        foreach ($buckets->bucketIds() as $bucketId) {
            $this->bucketsCache->remove($bucketId);
        }

        return new Bucket($nextBucketId, $this->bucketsCache->get($nextBucketId));
    }
}
