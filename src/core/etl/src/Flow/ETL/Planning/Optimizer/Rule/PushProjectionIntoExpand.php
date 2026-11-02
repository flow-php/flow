<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Optimizer\Rule;

use Flow\ETL\Operators\Processor\RequiredColumns;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\ReplaceNodes;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Values\Column\Backend;
use SplObjectStorage;

/**
 * Columns no consumer reads are not copied into the rows an expand emits.
 */
final readonly class PushProjectionIntoExpand implements Rule
{
    public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan
    {
        /** @var SplObjectStorage<Node\ExpandColumn, RequiredColumns> $demands */
        $demands = new SplObjectStorage();

        foreach ($plan->consumerInputs() as $input) {
            $required = RequiredColumns::all();

            for ($node = $input; $node->children() !== []; $node = $node->children()[0]) {
                if ($node instanceof Node\ExpandColumn) {
                    $demands[$node] = $demands->offsetExists($node) ? $demands[$node]->union($required) : $required;
                }

                $required = $node->requiredInputColumns($required);
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
