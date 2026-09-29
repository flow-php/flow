<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\Storage\SpillingBuckets;
use Flow\ETL\Config;
use Flow\ETL\Config\Grouping\GroupByAlgorithmBuilder;
use Flow\ETL\GroupBy;
use Flow\ETL\Processor;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Processor\PivotProcessor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\PruneEntriesTransformer;

use function array_values;

final readonly class GroupBySteps
{
    /**
     * @param null|GroupByAlgorithmBuilder $algorithm null defers to configuration; a builder pins the
     *                                                algorithm for this operation and skips any automatic choice
     *
     * @return list<Processor|Transformer>
     */
    public static function of(GroupBy $groupBy, Config $config, ?GroupByAlgorithmBuilder $algorithm = null): array
    {
        $grouping =
            $algorithm?->build($config->cache->localFilesystemCacheDir, $config->backend()) ?? $config->grouping;
        if ($groupBy->isPivot()) {
            return [new PivotProcessor($groupBy, $grouping->bucketing->batchSize)];
        }

        $steps = [];
        $aggregatorReferences = $groupBy->aggregations()->references();

        if ($aggregatorReferences !== null) {
            $pruned = [];

            foreach ([...$groupBy->references(), ...$aggregatorReferences] as $ref) {
                $pruned[$ref->base()] ??= $ref->base();
            }

            $steps[] = new PruneEntriesTransformer(...array_values($pruned));
        }

        $buckets = new Buckets(SpillingBuckets::around(
            $grouping->bucketing,
            $grouping->memoryLimit,
            $config->backend(),
        ));
        $steps[] = new GroupByAggregationProcessor(
            $groupBy,
            new HashBucketing(
                $groupBy->references(),
                $grouping->bucketing->bucketsCount,
                new NativeHasher(),
                $config->randomValueGenerator(),
                'group-by',
            ),
            $buckets,
            $grouping->memoryLimit,
            $grouping->bucketing->batchSize,
        );

        return $steps;
    }
}
