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
use function random_bytes;

/**
 * External sorting is explained here:.
 *
 * https://medium.com/outco/how-to-merge-k-sorted-arrays-c35d87aa298e
 * https://web.archive.org/web/20150202022830/http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
 */
final class ExternalSort implements SortingAlgorithm
{
    /**
     * @param BucketsCache $bucketsCache
     * @param int<1,max> $bucketsCount - Buckets counts defines how many rows are compared at time. Higher number can reduce IO but increase memory consumption
     * @param int<1,max> $batchSize
     * @param int<1,max> $runSize
     */
    public function __construct(
        private readonly BucketsCache $bucketsCache,
        private readonly int $bucketsCount = 10,
        private readonly int $batchSize = 1_000,
        private readonly int $runSize = 10_000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0, given: ' . $this->bucketsCount);
        }

        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }

        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->runSize < 1) {
            throw new InvalidArgumentException('Run size must be greater than 0, given: ' . $this->runSize);
        }
    }

    /**
     * @return \Generator<Rows>
     */
    public function sortGenerator(Generator $rows, FlowContext $context, References $refs): Generator
    {
        $buckets = [];

        foreach ($this->createBucketsFromGenerator($rows, $refs) as $bucket) {
            $buckets[] = $bucket;
        }

        while (count($buckets) > $this->bucketsCount) {
            $merged = [];

            foreach (array_chunk($buckets, $this->bucketsCount) as $chunk) {
                $merged[] = count($chunk) === 1 ? $chunk[0] : $this->sortBuckets(new Buckets($chunk), $refs);
            }

            $buckets = $merged;
        }

        $finalBuckets = new Buckets($buckets);

        yield from $this->batchRows($finalBuckets->sort(...$refs->all()));

        foreach ($finalBuckets->bucketIds() as $bucketId) {
            $this->bucketsCache->remove($bucketId);
        }
    }

    /**
     * @param \Generator<Row> $rows
     *
     * @return \Generator<Rows>
     */
    private function batchRows(Generator $rows): Generator
    {
        /** @var array<Row> $buffer */
        $buffer = [];

        foreach ($rows as $row) {
            $buffer[] = $row;

            if (count($buffer) >= $this->batchSize) {
                yield new Rows(...$buffer);
                $buffer = [];
            }
        }

        if ($buffer !== []) {
            yield new Rows(...$buffer);
        }
    }

    /**
     * @param \Generator<Rows> $generator
     *
     * @return \Generator<int, Bucket>
     */
    private function createBucketsFromGenerator(Generator $generator, References $refs): Generator
    {
        /** @var array<Row> $buffer */
        $buffer = [];

        foreach ($generator as $batch) {
            foreach ($batch as $row) {
                $buffer[] = $row;

                if (count($buffer) >= $this->runSize) {
                    yield $this->spillRun(new Rows(...$buffer), $refs);
                    $buffer = [];
                }
            }
        }

        if ($buffer !== []) {
            yield $this->spillRun(new Rows(...$buffer), $refs);
        }
    }

    private function sortBuckets(Buckets $sortBuckets, References $refs): Bucket
    {
        $this->bucketsCache->set($nextBucketId = bin2hex(random_bytes(16)), $sortBuckets->sort(...$refs->all()));

        foreach ($sortBuckets->bucketIds() as $bucketId) {
            $this->bucketsCache->remove($bucketId);
        }

        return new Bucket($nextBucketId, $this->bucketsCache->get($nextBucketId));
    }

    private function spillRun(Rows $run, References $refs): Bucket
    {
        $bucketId = bin2hex(random_bytes(16));
        $this->bucketsCache->set($bucketId, $run->sortBy(...$refs));

        return new Bucket($bucketId, $this->bucketsCache->get($bucketId));
    }
}
