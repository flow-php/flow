<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Optimizer;

use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\JoinsFrame;
use Flow\ETL\Planning\Logical\Rewrite;
use Flow\ETL\Values\Column\Backend;

/**
 * Optimizes a join's right side as the plan it is - the outer plan's rewrites never reach into it.
 */
final readonly class JoinSides implements Rewrite
{
    public function __construct(
        private Optimizer $optimizer,
        private Backend $backend,
    ) {}

    public function of(Node $original, Node $rebuilt): Node
    {
        if (!$rebuilt instanceof JoinsFrame) {
            return $rebuilt;
        }

        return $rebuilt->withChildren([
            $rebuilt->children()[0],
            $this->optimizer->optimize(new LogicalPlan($rebuilt->right()), $this->backend)->root,
        ]);
    }
}
