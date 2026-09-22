<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer\Rule;

use Flow\ETL\FlowContext;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node\Count;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Processor\CountingProcessor;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function Flow\ETL\DSL\from_rows;

final readonly class CountFromStatistics implements Rule
{
    /**
     * A count straight over a source that knows its rows exactly reads a row holding that number instead. Any node
     * in between runs code that may fail or skip a batch, and a pushed limit or partition filter changes the rows
     * the statistics describe, so those plans are counted by running them.
     */
    public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan
    {
        $count = $plan->root instanceof Result ? $plan->root->children()[0] : null;
        $read = $count instanceof Count ? $count->children()[0] : null;

        if (!$read instanceof Read || $read->limit() !== null || !$read->pathFilter() instanceof OnlyFiles) {
            return $plan;
        }

        $rows = $read->extractor()->statistics()->rows;

        if ($rows->atMost === null || $rows->atMost !== $rows->estimate || $rows->relativeError !== 0.0) {
            return $plan;
        }

        return new LogicalPlan(new Result(new Read(from_rows((new CountingProcessor())->rows($rows->atMost)))));
    }
}
