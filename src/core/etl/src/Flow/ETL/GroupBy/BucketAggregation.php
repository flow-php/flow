<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Rows;
use Generator;

final readonly class BucketAggregation
{
    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    /**
     * Reached only when the plan could not bind, so the shape comes from the first batch that carries rows.
     *
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function aggregate(Generator $rows, FlowContext $context, GroupBy $groupBy): Generator
    {
        $groups = null;

        foreach ($rows as $batch) {
            if ($groups === null) {
                if (!$batch->count()) {
                    continue;
                }

                $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $batch->schema()));
            }

            $groups->accumulate($batch, $context);
        }

        if ($groups !== null) {
            yield from $groups->flush($this->batchSize);
        }
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function aggregateBound(
        Generator $rows,
        FlowContext $context,
        GroupBy $groupBy,
        GroupByShape $shape,
    ): Generator {
        $groups = new AggregatedGroups($groupBy, $shape);

        foreach ($rows as $batch) {
            $groups->accumulate($batch, $context);
        }

        yield from $groups->flush($this->batchSize);
    }
}
