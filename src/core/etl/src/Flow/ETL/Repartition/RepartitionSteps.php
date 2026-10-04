<?php

declare(strict_types=1);

namespace Flow\ETL\Repartition;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\Storage\SpillingBuckets;
use Flow\ETL\Config;
use Flow\ETL\Config\Repartition\RepartitionAlgorithmBuilder;
use Flow\ETL\Processor;
use Flow\ETL\Processor\RepartitionProcessor;
use Flow\ETL\Row\References;

final readonly class RepartitionSteps
{
    /**
     * @param null|RepartitionAlgorithmBuilder $algorithm null defers to configuration; a builder pins
     *                                                    the algorithm for this operation
     *
     * @return list<Processor>
     */
    public static function of(References $by, Config $config, ?RepartitionAlgorithmBuilder $algorithm = null): array
    {
        $repartition =
            $algorithm?->build($config->cache->localFilesystemCacheDir, $config->backend()) ?? $config->repartition;
        $buckets = new Buckets(SpillingBuckets::around(
            $repartition->bucketing,
            $repartition->memoryLimit,
            $config->backend(),
        ));

        return [
            new RepartitionProcessor(
                $by,
                new HashBucketing(
                    $by->all(),
                    $repartition->bucketing->bucketsCount,
                    new NativeHasher(),
                    $config->randomValueGenerator(),
                    'repartition',
                ),
                $buckets,
                $repartition->memoryLimit,
            ),
        ];
    }
}
