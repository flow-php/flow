<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer\Rule;

use Flow\ETL\Extractor\File\FileExtractor;
use Flow\ETL\Filesystem\ScalarFunctionFilter;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Conjuncts;
use Flow\ETL\Function\ReferencedColumns;
use Flow\ETL\Optimizer\FilterWalk;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\ReplaceLeaf;

use function array_diff;

/**
 * Offers every deterministic part of an AND to the file source as a predicate it may skip data with, and evaluates a
 * partition-only part once per FILE instead of once per row: the file never opens. The Filter node stays, so a
 * source that reads more than the pushed predicate admits still returns the right rows. An OR is pushed only as a
 * whole.
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
        $consumerInputs = $plan->consumerInputs();
        $walk = new FilterWalk();
        $columns = new ReferencedColumns();
        $conjuncts = new Conjuncts();
        $names = $partitions->references()->names();
        $read = $leaf;

        for ($node = $plan->root; $node->children() !== []; $node = $node->children()[0]) {
            if (!$node instanceof Node\Filter || !$walk->reachedByEveryConsumer($node, ...$consumerInputs)) {
                continue;
            }

            foreach ($conjuncts->of($node->function) as $conjunct) {
                if (!$conjunct->deterministic()) {
                    continue;
                }

                $atLeaf = $walk->predicateAtLeaf($node, $leaf, $conjunct);

                if ($atLeaf === null) {
                    continue;
                }

                $refs = $columns->in($atLeaf);

                if ($refs->all() === []) {
                    continue;
                }

                if (array_diff($refs->names(), $names) === []) {
                    $read = $read->withPathFilter(new ScalarFunctionFilter($atLeaf, $partitions, $context));
                }

                $read = $read->withFilter($atLeaf);
            }
        }

        return $read === $leaf ? $plan : $plan->transformUp(new ReplaceLeaf($leaf, $read));
    }
}
