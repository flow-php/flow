<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer\Rule;

use Flow\ETL\FlowContext;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Optimizer\TopNRewrite;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Node\TopN;

/**
 * Limit(Sort(x), n) -> TopN(x, n): the sort keeps n rows instead of the whole input. A shared Sort stays for its
 * other consumers. An external sort bounds memory by its run size, so a larger n keeps the external sort.
 */
final readonly class CombineSortAndLimit implements Rule
{
    public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan
    {
        return $plan->transformUp(new TopNRewrite($context));
    }
}
