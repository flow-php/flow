<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Rows;
use Generator;

final readonly class PivotAggregation
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
     * Reached only when the plan could not bind, so the shape comes from the first batch.
     *
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function aggregate(Generator $rows, FlowContext $context, GroupBy $groupBy): Generator
    {
        if ($groupBy->aggregations()->count() === 0) {
            throw new RuntimeException('Pivot requires exactly one aggregation');
        }

        $table = null;

        foreach ($rows as $batch) {
            $table ??= new PivotedTable($groupBy, PivotShape::of($groupBy, $batch->schema()));
            $table->accumulate($batch, $context);
        }

        if ($table !== null) {
            yield from $table->flush($this->batchSize, $context);
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
        PivotShape $shape,
    ): Generator {
        $table = new PivotedTable($groupBy, $shape);

        foreach ($rows as $batch) {
            $table->accumulate($batch, $context);
        }

        yield from $table->flush($this->batchSize, $context);
    }
}
