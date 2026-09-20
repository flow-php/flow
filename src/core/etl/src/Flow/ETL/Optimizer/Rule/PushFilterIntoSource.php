<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer\Rule;

use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Filesystem\ScalarFunctionFilter;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\All;
use Flow\ETL\Function\ReferencedColumns;
use Flow\ETL\Optimizer\FilterWalk;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\ReplaceLeaf;

use function array_diff;

/**
 * Evaluate a partition-only predicate once per FILE instead of once per row: the file never opens. The Filter node
 * stays, so a source that lists more than the pushed filter admits still returns the right rows. The parts of an AND
 * that read only partition columns are pushed one by one; an OR is pushed only as a whole.
 */
final readonly class PushFilterIntoSource implements Rule
{
    public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan
    {
        $leaf = $plan->source();
        $extractor = $leaf->extractor();

        if (!$extractor instanceof FileExtractor) {
            return $plan;
        }

        $partitions = $extractor->partitionSchema();

        if ($partitions->count() === 0) {
            return $plan;
        }

        $consumerInputs = $plan->consumerInputs();
        $walk = new FilterWalk();
        $columns = new ReferencedColumns();
        $names = $partitions->references()->names();
        $read = $leaf;

        for ($node = $plan->root; $node->children() !== []; $node = $node->children()[0]) {
            if (!$node instanceof Node\Filter || !$walk->reachedByEveryConsumer($node, ...$consumerInputs)) {
                continue;
            }

            foreach ($node->function instanceof All ? $node->function->children() : [$node->function] as $conjunct) {
                if (!$conjunct->deterministic()) {
                    continue;
                }

                $atLeaf = $walk->predicateAtLeaf($node, $leaf, $conjunct);

                if ($atLeaf === null) {
                    continue;
                }

                $refs = $columns->in($atLeaf);

                if ($refs->all() === [] || array_diff($refs->names(), $names) !== []) {
                    continue;
                }

                $read = $read->withPathFilter(new ScalarFunctionFilter($atLeaf, $partitions, $context));
            }
        }

        return $read === $leaf ? $plan : $plan->transformUp(new ReplaceLeaf($leaf, $read));
    }
}
