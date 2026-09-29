<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Config;
use Flow\ETL\Config\Sort\MemorySortConfig;
use Flow\ETL\Config\Sort\SortAlgorithmBuilder;
use Flow\ETL\Processor;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Processor\MergeSortProcessor;
use Flow\ETL\Row\References;

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
        $external = self::external($refs, $config, $algorithm);

        return $external === null ? [new MemorySortProcessor($refs)] : [new MergeSortProcessor($external)];
    }

    /**
     * @param null|SortAlgorithmBuilder $algorithm null defers to configuration
     */
    public static function external(
        References $refs,
        Config $config,
        ?SortAlgorithmBuilder $algorithm = null,
    ): ?ExternalSort {
        $sort = $algorithm?->build($config->cache->localFilesystemCacheDir, $config->backend()) ?? $config->sort;

        if ($sort instanceof MemorySortConfig) {
            return null;
        }

        return new ExternalSort(
            $refs,
            new Buckets($sort->bucketing->storage),
            // without the ??, a merge side built on its own would silently spill to local disk for a MemoryBuckets user
            new Buckets($sort->merge ?? $sort->bucketing->storage),
            $config->randomValueGenerator(),
            $sort->memoryLimit,
            $sort->bucketing->bucketsCount,
            $sort->bucketing->batchSize,
        );
    }
}
