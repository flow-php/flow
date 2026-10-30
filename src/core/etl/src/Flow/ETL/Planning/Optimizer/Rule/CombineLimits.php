<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Optimizer\Rule;

use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Limit;
use Flow\ETL\Planning\Logical\Rewrite;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Values\Column\Backend;

use function min;

/**
 * Limit(Limit(x, m), n) -> Limit(x, min(n, m)).
 */
final readonly class CombineLimits implements Rewrite, Rule
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

        $child = $rebuilt->children()[0];

        return $child instanceof Limit
            ? new Limit($child->children()[0], min($rebuilt->limit, $child->limit))
            : $rebuilt;
    }
}
