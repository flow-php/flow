<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer\Rule;

use Flow\ETL\FlowContext;
use Flow\ETL\Optimizer\ProjectionWalk;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\ReplaceLeaf;
use Flow\ETL\Plan\RequiredColumns;
use SplObjectStorage;

use function arsort;
use function spl_object_id;

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

        $depths = [];
        $targets = [];

        foreach ($demands as $node) {
            if ($demands[$node]->isAll()) {
                continue;
            }

            $id = spl_object_id($node);
            $targets[$id] = $node;
            $depths[$id] = 0;

            for ($below = $node; $below->children() !== []; $below = $below->children()[0]) {
                $depths[$id]++;
            }
        }

        // TransformUp hands Rewrite::of() the node rebuilt over its new children, so a node whose descendant was
        // replaced first would no longer be the target; the one farthest from the leaf goes first
        arsort($depths);

        foreach ($depths as $id => $_) {
            $plan = $plan->transformUp(
                new ReplaceLeaf($targets[$id], $targets[$id]->withCarries($demands[$targets[$id]])),
            );
        }

        return $plan;
    }
}
