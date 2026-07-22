<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Config;
use Flow\ETL\DataFrame;
use Flow\ETL\Processor;
use Flow\ETL\Processor\HashJoinProcessor;

/**
 * @internal
 */
final readonly class JoinSteps
{
    /**
     * @return list<Processor>
     */
    public static function of(DataFrame $right, Expression $on, Join $type, Config $config): array
    {
        return [
            new HashJoinProcessor(
                $right,
                $on,
                $type,
                new Buckets($config->join->bucketing->storage),
                new Buckets($config->join->bucketing->storage),
                $config->randomValueGenerator(),
                $config->join->bucketing->bucketsCount,
                $config->join->bucketing->batchSize,
            ),
        ];
    }
}
