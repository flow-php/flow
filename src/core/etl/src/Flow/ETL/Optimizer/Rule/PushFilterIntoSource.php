<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer\Rule;

use Flow\ETL\Extractor\File\FileExtractor;
use Flow\ETL\Filesystem\ScalarFunctionFilter;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\All;
use Flow\ETL\Function\Conjuncts;
use Flow\ETL\Function\ReferencedColumns;
use Flow\ETL\Optimizer\FilterWalk;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\ReplaceNodes;
use SplObjectStorage;

use function array_diff;
use function array_reverse;
use function count;

/**
 * Moves every deterministic part of an AND into the file source, which returns only the rows matching it; a
 * partition-only part is also pushed as a path filter, so a file it rejects is never listed. A pushed part leaves its
 * Filter node, and a node left with no part is removed. An OR is pushed only as a whole. Nodes are pushed leaf first;
 * a part kept in a node stops the push of every part after it and of every node above it.
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
        /** @var SplObjectStorage<Node, Node> $replacements */
        $replacements = new SplObjectStorage();
        $removals = [];
        $filters = [];

        for ($node = $plan->root; $node->children() !== []; $node = $node->children()[0]) {
            if ($node instanceof Node\Filter && $walk->reachedByEveryConsumer($node, ...$consumerInputs)) {
                $filters[] = $node;
            }
        }

        // leaf first, so the source evaluates a lower filter before the one above it relies on what it removed
        foreach (array_reverse($filters) as $filter) {
            $parts = $conjuncts->of($filter->function);
            $kept = [];

            foreach ($parts as $conjunct) {
                // a conjunct after a kept one would be evaluated on rows the kept one removes
                $atLeaf = $kept === [] && $conjunct->deterministic()
                    ? $walk->predicateAtLeaf($filter, $leaf, $conjunct)
                    : null;

                if ($atLeaf === null) {
                    $kept[] = $conjunct;

                    continue;
                }

                $referenced = $columns->in($atLeaf)->names();

                if ($referenced !== [] && array_diff($referenced, $names) === []) {
                    $read = $read->withPathFilter(new ScalarFunctionFilter($atLeaf, $partitions, $context));
                }

                $read = $read->withFilter($atLeaf);
            }

            if ($kept === []) {
                $removals[] = $filter;
            } elseif (count($kept) !== count($parts)) {
                $replacements[$filter] = new Node\Filter(
                    $filter->children()[0],
                    count($kept) === 1 ? $kept[0] : new All(...$kept),
                );
            }

            // a filter above a kept conjunct would be evaluated on rows that conjunct removes
            if ($kept !== []) {
                break;
            }
        }

        if ($read !== $leaf) {
            $replacements[$leaf] = $read;
        }

        return $replacements->count() === 0 && $removals === []
            ? $plan
            : $plan->transformUp(new ReplaceNodes($replacements, $removals));
    }
}
