<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\SortedRunBucketing;
use Flow\ETL\Config;
use Flow\ETL\Config\Sort\MemorySortConfig;
use Flow\ETL\Config\Sort\SortAlgorithmBuilder;
use Flow\ETL\Processor;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Processor\MergeSortProcessor;
use Flow\ETL\Row\References;

/**
 * @internal
 */
final readonly class SortSteps
{
    /**
     * @param null|SortAlgorithmBuilder $algorithm null defers to configuration; a builder pins the algorithm
     *                                             for this operation and skips any automatic choice
     *
     * @return list<Processor>
     */
    public static function of(References $refs, Config $config, ?SortAlgorithmBuilder $algorithm = null): array
    {
        $sort = $algorithm?->build($config->cache->localFilesystemCacheDir) ?? $config->sort;

        if ($sort instanceof MemorySortConfig) {
            return [new MemorySortProcessor($refs)];
        }

        $random = $config->randomValueGenerator();
        $spill = new Buckets($sort->bucketing->storage);
        // without the ??, a merge side built on its own would silently spill to local disk for a MemoryBuckets user
        $merge = new Buckets($sort->merge ?? $sort->bucketing->storage);

        return [
            new BucketingProcessor(new SortedRunBucketing($refs->all(), $sort->runSize, $random), $spill),
            new MergeSortProcessor(
                $refs,
                $spill,
                $merge,
                $random,
                $sort->bucketing->bucketsCount,
                $sort->bucketing->batchSize,
            ),
        ];
    }
}
