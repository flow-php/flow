<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Rewrite;
use Flow\ETL\Plan\TransformUp;
use Flow\ETL\Planner\Rule;

/**
 * Rewrites the root with its own TransformUp and builds a new LogicalPlan around it instead of going through
 * LogicalPlan::transformUp(). With one root every sink is a child of that root, so even this shortcut keeps them -
 * the test pins that a rule cannot un-share a prefix any more.
 */
final readonly class SpineCopyingRule implements Rewrite, Rule
{
    public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan
    {
        return new LogicalPlan((new TransformUp())->of($plan->root, $this));
    }

    public function of(Node $node): Node
    {
        return $node instanceof Read ? new Read($node->extractor(), $node->scan()) : $node;
    }
}
