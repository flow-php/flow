<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\{
    Exception\InvalidArgumentException,
    Extractor,
    FlowContext,
    Pipeline,
    Pipeline\BatchingPipeline,
    Row\References,
    Sort\ExternalSort\Bucket,
    Sort\ExternalSort\Buckets,
    Sort\ExternalSort\BucketsCache};

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
     * @param int<1,max> $bucketsCount - Buckets counts defines how many rows are compared at time. Higher number can reduce IO but increase memory consumption
     */
    public function __construct(
        private readonly BucketsCache $bucketsCache,
        private readonly int $bucketsCount = 10,
    ) {
        if ($this->bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0, given: ' . $this->bucketsCount);
        }
    }

    public function sortBy(Pipeline $pipeline, FlowContext $context, References $refs) : Extractor
    {
        $sortedBuckets = [];

        foreach ($this->createBuckets($pipeline, $context, $refs) as $buckets) {
            $sortedBuckets[] = $this->sortBuckets($buckets, $refs);
        }

        return new Extractor\SortBucketsExtractor($this->mergeBuckets($sortedBuckets, $refs), \abs($this->batchSize), $this->bucketsCache);
    }

    /**
     * @return \Generator<int, Buckets>
     */
    private function createBuckets(Pipeline $pipeline, FlowContext $context, References $refs) : \Generator
    {
        /**
         * @var array<string, Bucket> $buckets
         */
        $buckets = [];

        $generator = $pipeline->process($context);

        generator:
        foreach ($generator as $rows) {
            if ($this->batchSize === -1) {
                $this->batchSize = $rows->count();

                /**
                 * Batch size below 500 will generate too many buckets and will increase IO.
                 */
                if ($this->batchSize < 500) {
                    $generator->rewind();
                    $generator = (new BatchingPipeline($pipeline, 500))->process($context);

                    goto generator;
                }
            }

            $bucketId = \bin2hex(\random_bytes(16));
            $this->bucketsCache->set($bucketId, $rows->sortBy(...$refs));
            $buckets[] = new Bucket($bucketId, $this->bucketsCache->get($bucketId));

            if (\count($buckets) >= $this->bucketsCount) {
                yield new Buckets($buckets);
                $buckets = [];
            }
        }

        if (\count($buckets) > 0) {
            yield new Buckets($buckets);
        }
    }

    /**
     * @param array<Bucket> $buckets
     */
    private function mergeBuckets(array $buckets, References $refs) : array
    {
        $bucketChunks = \array_chunk($buckets, $this->bucketsCount, true);

        $buckets = [];

        foreach ($bucketChunks as $runBuckets) {
            $buckets[] = $this->sortBuckets(new Buckets($runBuckets), $refs);
        }

        while (\count($buckets) > 1) {
            $buckets = $this->mergeBuckets($buckets, $refs);
        }

        return $buckets;
    }

    /**
     * @param Buckets $sortBuckets
     * @param References $refs
     */
    private function sortBuckets(Buckets $sortBuckets, References $refs) : Bucket
    {
        $this->bucketsCache->set($nextBucketId = \bin2hex(\random_bytes(16)), $sortBuckets->sort(...$refs->all()));

        foreach ($sortBuckets->bucketIds() as $bucketId) {
            $this->bucketsCache->remove($bucketId);
        }

        return new Bucket($nextBucketId, $this->bucketsCache->get($nextBucketId));
    }
}
