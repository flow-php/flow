<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Optimizer;

use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Values\Column\Backend;

interface Rule
{
    /**
     * Rewrite the plan, or return it unchanged. A rule never mutates the plan it is given.
     *
     * A rule that rewrites nodes goes through LogicalPlan::transformUp(): its one memo rewrites a node every
     * consumer shares exactly once, so the Result and the writes keep sharing it.
     */
    public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan;
}
