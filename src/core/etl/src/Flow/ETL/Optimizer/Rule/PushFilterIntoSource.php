<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer\Rule;

use Flow\ETL\Extractor\File\FileExtractor;
use Flow\ETL\Filesystem\ScalarFunctionFilter;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\All;
use Flow\ETL\Function\Conjuncts;
use Flow\ETL\Function\ReferencedColumns;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Optimizer\FilterWalk;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\ReplaceNode;
use SplObjectStorage;

use function array_diff;
use function array_reverse;
use function count;

/**
 * Moves every deterministic part of an AND into the file source, which returns only the rows matching it, and
 * evaluates a partition-only part once per FILE instead of once per row: the file never opens. A pushed part leaves
 * its Filter node, and a node left with no part is removed. An OR is pushed only as a whole. Nodes are pushed leaf
 * first; a part kept in a node stops the push of every part after it and of every node above it.
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
        /** @var SplObjectStorage<Node\Filter, list<ScalarFunction>> $remaining */
        $remaining = new SplObjectStorage();

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
                $refs = $atLeaf === null ? [] : $columns->in($atLeaf)->names();

                if ($atLeaf === null || $refs === []) {
                    $kept[] = $conjunct;

                    continue;
                }

                if (array_diff($refs, $names) === []) {
                    $read = $read->withPathFilter(new ScalarFunctionFilter($atLeaf, $partitions, $context));
                }

                $read = $read->withFilter($atLeaf);
            }

            if (count($kept) !== count($parts)) {
                $remaining[$filter] = $kept;
            }

            // a filter above a kept conjunct would be evaluated on rows that conjunct removes
            if ($kept !== []) {
                break;
            }
        }

        // TransformUp hands Rewrite::of() the node rebuilt over its new children, so a Filter below one already
        // replaced would no longer be the target; root first is the order they go in
        foreach ($filters as $filter) {
            if (!$remaining->offsetExists($filter)) {
                continue;
            }

            $plan = $plan->transformUp(new ReplaceNode($filter, match (count($remaining[$filter])) {
                0 => $filter->children()[0],
                1 => new Node\Filter($filter->children()[0], $remaining[$filter][0]),
                default => new Node\Filter($filter->children()[0], new All(...$remaining[$filter])),
            }));
        }

        return $read === $leaf ? $plan : $plan->transformUp(new ReplaceNode($leaf, $read));
    }
}
