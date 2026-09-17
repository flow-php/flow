<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer;

use Flow\ETL\FlowContext;
use Flow\ETL\Plan\LogicalPlan;

interface Rule
{
    /**
     * Rewrite the plan, or return it unchanged. A rule never mutates the plan it is given.
     *
     * A rule that rewrites nodes goes through LogicalPlan::transformUp(): its one memo rewrites a node every
     * consumer shares exactly once, so the Result and the sinks keep sharing it.
     *
     * A rule runs inside Planner::plan(), which is also the method a SideInput subtree and an inlined NestedPlan's
     * plan recurse into - so every rule re-applies inside a nested plan with THAT plan's context.
     */
    public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan;
}
