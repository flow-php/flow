<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Config;
use Flow\ETL\Config\Join\JoinAlgorithmBuilder;
use Flow\ETL\DataFrame;
use Flow\ETL\Processor;
use Flow\ETL\Processor\HashJoinProcessor;

/**
 * @internal
 */
final readonly class JoinSteps
{
    /**
     * @param null|JoinAlgorithmBuilder $algorithm null defers to configuration; a builder pins the algorithm
     *                                             for this operation and skips any automatic choice
     *
     * @return list<Processor>
     */
    public static function of(
        DataFrame $right,
        Expression $on,
        Join $type,
        Config $config,
        ?JoinAlgorithmBuilder $algorithm = null,
    ): array {
        $join = $algorithm?->build($config->cache->localFilesystemCacheDir) ?? $config->join;

        return [
            new HashJoinProcessor(
                $right,
                $on,
                $type,
                new Buckets($join->bucketing->storage),
                new Buckets($join->bucketing->storage),
                $config->randomValueGenerator(),
                $join->bucketing->bucketsCount,
                $join->bucketing->batchSize,
            ),
        ];
    }
}
