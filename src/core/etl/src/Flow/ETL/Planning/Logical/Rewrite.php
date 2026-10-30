<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical;

interface Rewrite
{
    /**
     * The replacement for $original, given as $rebuilt over its rewritten children; $rebuilt itself when nothing
     * changes.
     *
     * Apply it to a plan through LogicalPlan::transformUp(): its memo rewrites a node every consumer shares
     * exactly once, so the shared prefix stays shared. It never reaches into a join's right side.
     */
    public function of(Node $original, Node $rebuilt): Node;
}
