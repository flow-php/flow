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

use function array_chunk;
use function bin2hex;
use function count;
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
    private const int MINIMUM_OUTPUT_BATCH_SIZE = 1_000;

    private const int RUN_SIZE = 10_000;

    private int $batchSize = -1;

    /**
     * @param BucketsCache $bucketsCache
     * @param int<1,max> $bucketsCount - Buckets counts defines how many rows are compared at time. Higher number can reduce IO but increase memory consumption
     */
    public function __construct(
        private readonly BucketsCache $bucketsCache,
        private readonly int $bucketsCount = 10,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0, given: ' . $this->bucketsCount);
        }
    }

    /**
     * @return \Generator<Rows>
     */
    public function sortGenerator(Generator $rows, FlowContext $context, References $refs): Generator
    {
        $sortedBuckets = [];

        foreach ($this->createBucketsFromGenerator($rows, $refs) as $buckets) {
            $sortedBuckets[] = $this->sortBuckets($buckets, $refs);
        }

        yield from $this->extractSortedBuckets($this->mergeBuckets($sortedBuckets, $refs));
    }

    /**
     * @param \Generator<Rows> $generator
     *
     * @return \Generator<int, Buckets>
     */
    private function createBucketsFromGenerator(Generator $generator, References $refs): Generator
    {
        /** @var array<Bucket> $buckets */
        $buckets = [];

        /** @var array<Row> $buffer */
        $buffer = [];

        foreach ($generator as $batch) {
            $this->batchSize = max($this->batchSize, $batch->count());

            foreach ($batch as $row) {
                $buffer[] = $row;

                if (count($buffer) >= self::RUN_SIZE) {
                    $batchRows = new Rows(...$buffer);
                    $buffer = [];

                    $bucketId = bin2hex(random_bytes(16));
                    $this->bucketsCache->set($bucketId, $batchRows->sortBy(...$refs));
                    $buckets[] = new Bucket($bucketId, $this->bucketsCache->get($bucketId));

                    if (count($buckets) >= $this->bucketsCount) {
                        yield new Buckets($buckets);
                        $buckets = [];
                    }
                }
            }
        }

        if ($buffer !== []) {
            $batchRows = new Rows(...$buffer);
            $bucketId = bin2hex(random_bytes(16));
            $this->bucketsCache->set($bucketId, $batchRows->sortBy(...$refs));
            $buckets[] = new Bucket($bucketId, $this->bucketsCache->get($bucketId));
        }

        if ($buckets !== []) {
            yield new Buckets($buckets);
        }
    }

    /**
     * @param array<Bucket> $sortBuckets
     *
     * @return \Generator<Rows>
     */
    private function extractSortedBuckets(array $sortBuckets): Generator
    {
        $outputBatchSize = max(self::MINIMUM_OUTPUT_BATCH_SIZE, $this->batchSize);

        foreach ($sortBuckets as $bucket) {
            /** @var array<Row> $buffer */
            $buffer = [];

            foreach ($bucket->rows as $row) {
                $buffer[] = $row;

                if (count($buffer) >= $outputBatchSize) {
                    yield new Rows(...$buffer);
                    $buffer = [];
                }
            }

            if ($buffer !== []) {
                yield new Rows(...$buffer);
            }

            $this->bucketsCache->remove($bucket->id);
        }
    }

    /**
     * @param array<Bucket> $buckets
     *
     * @return array<Bucket>
     */
    private function mergeBuckets(array $buckets, References $refs): array
    {
        $bucketChunks = array_chunk($buckets, $this->bucketsCount, true);

        $buckets = [];

        foreach ($bucketChunks as $runBuckets) {
            $buckets[] = $this->sortBuckets(new Buckets($runBuckets), $refs);
        }

        while (count($buckets) > 1) {
            $buckets = $this->mergeBuckets($buckets, $refs);
        }

        return $buckets;
    }

    private function sortBuckets(Buckets $sortBuckets, References $refs): Bucket
    {
        $this->bucketsCache->set($nextBucketId = bin2hex(random_bytes(16)), $sortBuckets->sort(...$refs->all()));

        foreach ($sortBuckets->bucketIds() as $bucketId) {
            $this->bucketsCache->remove($bucketId);
        }

        return new Bucket($nextBucketId, $this->bucketsCache->get($nextBucketId));
    }
}
