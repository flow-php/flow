<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer\Rule;

use Flow\ETL\FlowContext;
use Flow\ETL\Optimizer\ProjectionWalk;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\ReplaceNodes;
use Flow\ETL\Plan\RequiredColumns;
use SplObjectStorage;

/**
 * Columns no consumer reads are not copied into the rows an expand emits (Spark: ColumnPruning setting
 * Generate.unrequiredChildIndex).
 */
final readonly class PushProjectionIntoExpand implements Rule
{
    public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan
    {
        $walk = new ProjectionWalk();
        /** @var SplObjectStorage<Node\ExpandColumn, RequiredColumns> $demands */
        $demands = new SplObjectStorage();

        foreach ($plan->consumerInputs() as $input) {
            $required = RequiredColumns::all();

            for ($node = $input; $node->children() !== []; $node = $node->children()[0]) {
                if ($node instanceof Node\ExpandColumn) {
                    $demands[$node] = $demands->offsetExists($node) ? $demands[$node]->union($required) : $required;
                }

                $required = $walk->below($node, $required);
            }
        }

        /** @var SplObjectStorage<Node, Node> $replacements */
        $replacements = new SplObjectStorage();

        foreach ($demands as $node) {
            if (!$demands[$node]->isAll()) {
                $replacements[$node] = $node->withCarries($demands[$node]);
            }
        }

        return $replacements->count() === 0 ? $plan : $plan->transformUp(new ReplaceNodes($replacements));
    }
}
