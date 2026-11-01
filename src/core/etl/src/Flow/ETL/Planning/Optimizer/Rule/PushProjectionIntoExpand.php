<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Optimizer\Rule;

use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Functions\ReferencedColumns;
use Flow\ETL\Functions\ScalarFunction\UnpackResults;
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

                $required = $this->below($node, $required);
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

    /**
     * What the node's row input must hold so that $above holds what its consumers read.
     */
    public function below(Node $node, RequiredColumns $above): RequiredColumns
    {
        $refs = new ReferencedColumns();

        return match (true) {
            $node instanceof Node\Select => RequiredColumns::only(...References::init(...$node->columns)->names()),
            $node instanceof Node\Drop => $above->without(...References::init(...$node->columns)->names()),
            // an unpack writes "name.*", known only once bound: keep the demand, add what it reads
            ($node instanceof Node\WithColumn || $node instanceof Node\ExpandColumn)
                && $node->function instanceof UnpackResults
                => $above->with(...$refs->in($node->function)->names()),
            $node instanceof Node\WithColumn, $node instanceof Node\ExpandColumn => $above
                ->without($node->name())
                ->with(...$refs->in($node->function)->names()),
            $node instanceof Node\Filter => $above->with(...$refs->in($node->function)->names()),
            $node instanceof Node\Limit, $node instanceof Node\Offset => $above,
            default => RequiredColumns::all(),
        };
    }
}
