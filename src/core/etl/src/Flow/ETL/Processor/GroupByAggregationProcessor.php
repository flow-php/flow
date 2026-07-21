<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketShape;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\BucketAggregation;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Generator;

/**
 * Aggregates buckets spilled by BucketingProcessor, one bucket per incoming metadata Row.
 *
 * @internal
 */
final readonly class GroupByAggregationProcessor implements Processor
{
    public function __construct(
        private GroupBy $groupBy,
        private Buckets $buckets,
    ) {}

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        try {
            foreach ($rows as $metadata) {
                /** @var string $bucketId */
                $bucketId = $metadata->first()->valueOf(BucketShape::id->value);

                $aggregated = (new BucketAggregation())->aggregate(
                    $this->buckets->rows($bucketId),
                    $context,
                    $this->groupBy,
                );

                // re-key batches, yield from would restart keys at 0 for every bucket
                foreach ($aggregated as $aggregatedBatch) {
                    yield $aggregatedBatch;
                }
            }
        } finally {
            $this->buckets->clear();
        }
    }
}
