<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\{
    Extractor,
    FlowContext,
    Pipeline\BatchingPipeline,
    Pipeline\SynchronousPipeline,
    Row,
    Row\References,
    Rows,
    Sort\ExternalSort\Config,
    Sort\ExternalSort\SortBuckets,
    Sort\ExternalSort\SortRowCache};

/**
 * External sorting is explained here:.
 *
 * https://medium.com/outco/how-to-merge-k-sorted-arrays-c35d87aa298e
 * https://web.archive.org/web/20150202022830/http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
 *
 * There is still much space for optimization, for example currently heap is created from all parts that for
 * massive datasets with millions of small Rows might become a potential memory leak.
 * Ideally in that case, Rows should be merged in multiple runs with a limited heap.
 */
final class ExternalSort implements SortingAlgorithm
{
    private int $batchSize = 1;

    public function __construct(
        private readonly Extractor $extractor,
        private readonly SortRowCache $rowCache,
        private readonly Config $config = new Config(),
        private readonly int $iteration = 0
    ) {
    }

    /**
     * @param SortBuckets $sortBuckets
     * @param References $refs
     *
     * @return array<string, \Generator<Row>>
     */
    public function sortBuckets(SortBuckets $sortBuckets, References $refs) : array
    {
        /** @var array<string, \Generator<Row>> $sortedChunks */
        $sortedBuckets = [];

        $nextBucketId = \bin2hex(\random_bytes(16));
        $this->rowCache->set($nextBucketId, $sortBuckets->sort(...$refs->all()));

        foreach ($sortBuckets->bucketIds() as $bucketId) {
            $this->rowCache->remove($bucketId);
        }

        $sortedBuckets[$nextBucketId] = $this->rowCache->get($nextBucketId);

        return $sortedBuckets;
    }

    public function sortBy(FlowContext $context, References $refs) : Extractor
    {
        $sortedBuckets = [];

        foreach ($this->createSortBuckets($context, $refs) as $sortBuckets) {
            $sortedBuckets[] = $this->sortBuckets($sortBuckets, $refs);
        }

        return new Extractor\SortRowCacheExtractor($this->mergeSortedBuckets(\array_merge(...$sortedBuckets), $refs), $this->batchSize, $this->rowCache);
    }

    /**
     * @return \Generator<int, SortBuckets>
     */
    private function createSortBuckets(FlowContext $context, References $refs) : \Generator
    {
        /**
         * @var array<string, \Generator<Row>> $sortedRowsGenerators
         */
        $sortedRowsGenerators = [];

        foreach ((new BatchingPipeline(new SynchronousPipeline($this->extractor), $this->config->sortBucketMaxSize))->process($context) as $rows) {
            $this->batchSize = \max($this->batchSize, $rows->count());
            $partId = \bin2hex(\random_bytes(16));
            $this->rowCache->set($partId, $rows->sortBy(...$refs));
            $sortedRowsGenerators[$partId] = $this->rowCache->get($partId);

            if (\count($sortedRowsGenerators) >= $this->config->sortBucketsCount) {
                yield new SortBuckets($sortedRowsGenerators);
                $sortedRowsGenerators = [];
            }
        }

        if (\count($sortedRowsGenerators) > 0) {
            yield new SortBuckets($sortedRowsGenerators);
        }
    }

    private function mergeSortedBuckets(array $sortedBuckets, References $refs) : array
    {
        if (\count($sortedBuckets) > $this->config->sortBucketsCount) {
            $runs = \array_chunk($sortedBuckets, $this->config->sortBucketsCount, true);
        } else {
            $runs = [$sortedBuckets];
        }

        $sortedBuckets = [];

        foreach ($runs as $runBuckets) {
            $sortedBuckets[] = $this->sortBuckets(new SortBuckets($runBuckets), $refs);
        }

        $sortedBuckets = \array_merge(...$sortedBuckets);

        while (\count($sortedBuckets) > 1) {
            $sortedBuckets = $this->mergeSortedBuckets($sortedBuckets, $refs);
        }

        return $sortedBuckets;
    }
}
