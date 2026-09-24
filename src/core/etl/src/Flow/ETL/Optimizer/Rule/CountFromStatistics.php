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
    public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan
    {
        $count = $plan->root instanceof Result ? $plan->root->children()[0] : null;
        $read = $count instanceof Count ? $count->children()[0] : null;

        if (!$read instanceof Read || $read->limit() !== null || !$read->pathFilter() instanceof OnlyFiles) {
            return $plan;
        }

        $rows = $read->extractor()->statistics()->rows->exactly();

        return $rows === null
            ? $plan
            : new LogicalPlan(new Result(new Read(from_rows((new CountingProcessor())->rows($rows)))));
    }
}
