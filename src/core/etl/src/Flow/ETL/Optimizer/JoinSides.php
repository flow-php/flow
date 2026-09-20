<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer;

use Flow\ETL\FlowContext;
use Flow\ETL\Optimizer;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\JoinsFrame;
use Flow\ETL\Plan\Rewrite;

/**
 * Optimizes a join's right side as the plan it is - the outer plan's rewrites never reach into it.
 */
final readonly class JoinSides implements Rewrite
{
    public function __construct(
        private Optimizer $optimizer,
        private FlowContext $context,
    ) {}

    public function of(Node $node): Node
    {
        if (!$node instanceof JoinsFrame) {
            return $node;
        }

        return $node->withChildren([
            $node->children()[0],
            $this->optimizer->optimize(new LogicalPlan($node->right()), $this->context)->root,
        ]);
    }
}
