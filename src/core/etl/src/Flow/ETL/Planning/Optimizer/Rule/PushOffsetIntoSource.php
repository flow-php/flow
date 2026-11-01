<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Optimizer\Rule;

use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Offset;
use Flow\ETL\Planning\Logical\ReplaceNodes;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Values\Column\Backend;
use SplObjectStorage;

use function array_reverse;
use function count;

/**
 * Runs after PushFilterIntoSource, only for single-consumer plans.
 */
final readonly class PushOffsetIntoSource implements Rule
{
    public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan
    {
        $leaf = $plan->source();
        $consumerInputs = $plan->consumerInputs();

        if (!$leaf->extractor() instanceof FileExtractor || count($consumerInputs) !== 1) {
            return $plan;
        }

        $spine = [];

        for ($node = $consumerInputs[0]; $node !== $leaf; $node = $node->children()[0]) {
            if ($node->children() === []) {
                return $plan;
            }

            $spine[] = $node;
        }

        $read = $leaf;
        $removals = [];

        foreach (array_reverse($spine) as $node) {
            if ($node instanceof Offset) {
                $read = $read->withOffset($node->offset);
                $removals[] = $node;
            } elseif (
                $node->rowCount() !== RowCount::preserving
                || $node->transparency() !== Transparency::transparent
            ) {
                break;
            }
        }

        if ($removals === []) {
            return $plan;
        }

        /** @var SplObjectStorage<Node, Node> $replacements */
        $replacements = new SplObjectStorage();
        $replacements[$leaf] = $read;

        return $plan->transformUp(new ReplaceNodes($replacements, $removals));
    }
}
