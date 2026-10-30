<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Optimizer\Rule;

use Flow\ETL\IO\Extractor\RowsExtractor;
use Flow\ETL\Operators\Processor\CountingProcessor;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node\Count;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Values\Column\Backend;

final readonly class CountFromStatistics implements Rule
{
    public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan
    {
        $count = $plan->root instanceof Result ? $plan->root->children()[0] : null;
        $read = $count instanceof Count ? $count->children()[0] : null;

        if (!$read instanceof Read || $read->criteria()->narrows()) {
            return $plan;
        }

        $rows = $read->extractor()->statistics()->rows->exactly();

        return $rows === null
            ? $plan
            : new LogicalPlan(new Result(new Read(new RowsExtractor(CountingProcessor::rows($rows, $backend)))));
    }
}
