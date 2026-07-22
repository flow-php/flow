<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketShape;
use Flow\ETL\Exception\InvalidArgumentException;
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
    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private GroupBy $groupBy,
        private Buckets $buckets,
        private int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        $aggregation = new BucketAggregation($this->batchSize);

        try {
            foreach ($rows as $metadata) {
                foreach ($metadata as $row) {
                    /** @var string $bucketId */
                    $bucketId = $row->valueOf(BucketShape::id->value);

                    $aggregated = $aggregation->aggregate($this->buckets->rows($bucketId), $context, $this->groupBy);

                    // re-key batches, yield from would restart keys at 0 for every bucket
                    foreach ($aggregated as $aggregatedBatch) {
                        yield $aggregatedBatch;
                    }
                }
            }
        } finally {
            $this->buckets->clear();
        }
    }
}
