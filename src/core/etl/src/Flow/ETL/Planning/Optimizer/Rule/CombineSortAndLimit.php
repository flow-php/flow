<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Optimizer\Rule;

use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Limit;
use Flow\ETL\Planning\Logical\Node\Sort;
use Flow\ETL\Planning\Logical\Node\TopN;
use Flow\ETL\Planning\Logical\Rewrite;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Values\Column\Backend;

/**
 * Limit(Sort(x), n) -> TopN(x, n): the sort keeps n rows instead of the whole input. A shared Sort stays for its
 * other consumers.
 */
final readonly class CombineSortAndLimit implements Rewrite, Rule
{
    public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan
    {
        return $plan->transformUp($this);
    }

    public function of(Node $original, Node $rebuilt): Node
    {
        if (!$rebuilt instanceof Limit) {
            return $rebuilt;
        }

        $sort = $rebuilt->children()[0];

        if (!$sort instanceof Sort) {
            return $rebuilt;
        }

        // a TopN past its memory limit falls back to the sort's own spilling path, so any limit is safe to rewrite
        return new TopN($sort->children()[0], $sort->columns, $rebuilt->limit, $sort->algorithm);
    }
}
