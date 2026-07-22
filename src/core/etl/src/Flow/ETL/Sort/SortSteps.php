<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\SortedRunBucketing;
use Flow\ETL\Config;
use Flow\ETL\Config\Sort\MemorySortConfig;
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
     * @return list<Processor>
     */
    public static function of(References $refs, Config $config): array
    {
        if ($config->sort instanceof MemorySortConfig) {
            return [new MemorySortProcessor($refs)];
        }

        $random = $config->randomValueGenerator();
        $buckets = new Buckets($config->sort->bucketing->storage);

        return [
            new BucketingProcessor(new SortedRunBucketing($refs->all(), $config->sort->runSize, $random), $buckets),
            new MergeSortProcessor(
                $refs,
                $buckets,
                $random,
                $config->sort->bucketing->bucketsCount,
                $config->sort->bucketing->batchSize,
            ),
        ];
    }
}
