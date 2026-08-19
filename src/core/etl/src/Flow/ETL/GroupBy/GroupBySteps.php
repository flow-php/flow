<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Config;
use Flow\ETL\GroupBy;
use Flow\ETL\Processor;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Processor\PivotProcessor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\PruneEntriesTransformer;

use function array_values;

/**
 * @internal
 */
final readonly class GroupBySteps
{
    /**
     * @return list<Processor|Transformer>
     */
    public static function of(GroupBy $groupBy, Config $config): array
    {
        if ($groupBy->isPivot()) {
            return [new PivotProcessor($groupBy, $config->grouping->bucketing->batchSize)];
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

        $buckets = new Buckets($config->grouping->bucketing->storage);
        $steps[] = new BucketingProcessor(
            new HashBucketing(
                $groupBy->references(),
                $config->grouping->bucketing->bucketsCount,
                new NativeHasher(),
                $config->randomValueGenerator(),
                'group-by',
                nullOnMissing: true,
            ),
            $buckets,
        );
        $steps[] = new GroupByAggregationProcessor($groupBy, $buckets, $config->grouping->bucketing->batchSize);

        return $steps;
    }
}
