<?php

declare(strict_types=1);

namespace Flow\ETL\Repartition;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Config;
use Flow\ETL\Config\Repartition\RepartitionAlgorithmBuilder;
use Flow\ETL\Processor;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\RepartitionProcessor;
use Flow\ETL\Row\References;

/**
 * @internal
 */
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
        $repartition = $algorithm?->build($config->cache->localFilesystemCacheDir) ?? $config->repartition;
        $buckets = new Buckets($repartition->bucketing->storage);

        return [
            new BucketingProcessor(
                new HashBucketing(
                    $by->all(),
                    $repartition->bucketing->bucketsCount,
                    new NativeHasher(),
                    $config->randomValueGenerator(),
                    'repartition',
                ),
                $buckets,
            ),
            new RepartitionProcessor($by, $buckets),
        ];
    }
}
