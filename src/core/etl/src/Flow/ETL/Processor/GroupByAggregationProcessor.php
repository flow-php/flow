<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketShape;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\BucketAggregation;
use Flow\ETL\GroupBy\GroupByShape;
use Flow\ETL\GroupBy\GroupKey;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

/**
 * Aggregates buckets spilled by BucketingProcessor, one bucket per incoming metadata Row.
 *
 * @internal
 */
final class GroupByAggregationProcessor implements Processor
{
    /**
     * The (input, aggregators, output) triple this step declares. Only bind() sets it; the unbound
     * path derives it from the first batch that carries rows.
     */
    private ?GroupByShape $shape = null;

    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private readonly GroupBy $groupBy,
        private readonly Buckets $buckets,
        private readonly int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    /**
     * @throws SchemaDefinitionNotFoundException
     */
    public function bind(Schema $input): BoundStep
    {
        $bound = new self($this->groupBy, $this->buckets, $this->batchSize);
        $bound->shape = GroupByShape::of($this->groupBy, $input);

        return new BoundStep($bound, $bound->shape->output);
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        $aggregation = new BucketAggregation($this->batchSize);
        $emitted = false;

        try {
            foreach ($rows as $metadata) {
                foreach ($metadata as $row) {
                    /** @var string $bucketId */
                    $bucketId = $row->get(BucketShape::id->value);
                    $bucket = $this->buckets->rows($bucketId);

                    $aggregated = $this->shape === null
                        ? $aggregation->aggregate($bucket, $context, $this->groupBy)
                        : $aggregation->aggregateBound($bucket, $context, $this->groupBy, $this->shape);

                    // re-key batches, yield from would restart keys at 0 for every bucket
                    foreach ($aggregated as $aggregatedBatch) {
                        $emitted = true;

                        yield $aggregatedBatch;
                    }
                }
            }

            // SQL's scalar aggregate: no input and no grouping key is one row of the initial accumulators,
            // not no row. Only reachable bound - an unbound plan has no schema to type the defaults with.
            if (!$emitted && $this->shape !== null && $this->groupBy->isGlobal()) {
                yield new Rows(
                    $this->shape->output,
                    $this->groupBy->aggregatedRow(
                        new GroupKey([]),
                        $this->shape->aggregators->cloned(),
                        $this->shape->output,
                    ),
                );
            }
        } finally {
            $this->buckets->clear();
        }
    }
}
