<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer\Rule;

use Flow\ETL\FlowContext;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Rewrite;

use function min;

/**
 * Limit(Limit(x, m), n) -> Limit(x, min(n, m)).
 */
final readonly class CombineLimits implements Rewrite, Rule
{
    public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan
    {
        return $plan->transformUp($this);
    }

    public function of(Node $node): Node
    {
        if (!$node instanceof Limit) {
            return $node;
        }

        $child = $node->children()[0];

        return $child instanceof Limit ? new Limit($child->children()[0], min($node->limit, $child->limit)) : $node;
    }
}
