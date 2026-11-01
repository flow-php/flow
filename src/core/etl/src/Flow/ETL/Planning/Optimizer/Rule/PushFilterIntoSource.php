<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Optimizer\Rule;

use Flow\ETL\Functions\All;
use Flow\ETL\Functions\Conjuncts;
use Flow\ETL\Functions\ReferencedColumns;
use Flow\ETL\Functions\ReferenceRename;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Filesystem\ScalarFunctionFilter;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\ReplaceNodes;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Values\Column\Backend;
use SplObjectStorage;

use function array_diff;
use function array_reverse;
use function count;

// a pushed part never throws, so it may run before the parts kept in PHP, which stay in written order
final readonly class PushFilterIntoSource implements Rule
{
    public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan
    {
        $leaf = $plan->source();
        $extractor = $leaf->extractor();

        if (!$extractor instanceof FileExtractor) {
            return $plan;
        }

        $partitions = $extractor->partitionSchema();
        $consumerInputs = $plan->consumerInputs();
        $columns = new ReferencedColumns();
        $conjuncts = new Conjuncts();
        $names = $partitions->names();
        $read = $leaf;
        /** @var SplObjectStorage<Node, Node> $replacements */
        $replacements = new SplObjectStorage();
        $removals = [];
        $filters = [];

        for ($node = $plan->root; $node->children() !== []; $node = $node->children()[0]) {
            if ($node instanceof Node\Filter && $this->reachedByEveryConsumer($node, ...$consumerInputs)) {
                $filters[] = $node;
            }
        }

        // leaf first, so the source evaluates a lower filter before the one above it relies on what it removed
        foreach (array_reverse($filters) as $filter) {
            $parts = $conjuncts->of($filter->function);
            $kept = [];

            foreach ($parts as $conjunct) {
                $atLeaf = $conjunct->deterministic() ? $this->predicateAtLeaf($filter, $leaf, $conjunct) : null;

                if ($atLeaf === null) {
                    $kept[] = $conjunct;

                    continue;
                }

                $referenced = $columns->in($atLeaf)->names();
                $partitionOnly = $referenced !== [] && array_diff($referenced, $names) === [];

                // a file the conjunct rejects holds no row its filter keeps
                if ($partitionOnly) {
                    $read = $read->withPathFilter(new ScalarFunctionFilter($atLeaf, $partitions, $backend));
                }

                if (!$partitionOnly && !$extractor->prunes($atLeaf)) {
                    $kept[] = $conjunct;

                    continue;
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
        }

        if ($read !== $leaf) {
            $replacements[$leaf] = $read;
        }

        return $replacements->count() === 0 && $removals === []
            ? $plan
            : $plan->transformUp(new ReplaceNodes($replacements, $removals));
    }

    /**
     * @param Node ...$consumerInputs LogicalPlan::consumerInputs()
     */
    public function reachedByEveryConsumer(Node\Filter $filter, Node ...$consumerInputs): bool
    {
        // a predicate pushed into the source narrows what every other consumer of it reads
        foreach ($consumerInputs as $input) {
            for ($node = $input; $node !== $filter; $node = $node->children()[0] ?? null) {
                if ($node === null) {
                    return false;
                }
            }
        }

        return true;
    }

    public function predicateAtLeaf(Node\Filter $filter, Read $leaf, ScalarFunction $predicate): ?ScalarFunction
    {
        $columns = new ReferencedColumns();

        for ($node = $filter->children()[0]; $node !== $leaf; $node = $node->children()[0]) {
            if ($node->children() === [] || $node->transparency() !== Transparency::transparent) {
                return null;
            }

            // the order two filters drop rows in does not change which rows survive both
            if ($node->rowCount() === RowCount::reducing && !$node instanceof Node\Filter) {
                return null;
            }

            $redefined = $node->redefines();

            foreach ($columns->in($predicate)->names() as $name) {
                if (!$redefined->defines($name)) {
                    continue;
                }

                $below = $redefined->aliasOf($name);
                $renamed = $below === null ? null : (new ReferenceRename($name, $below))->in($predicate);

                if (!$renamed instanceof ScalarFunction) {
                    return null;
                }

                $predicate = $renamed;
            }
        }

        return $predicate;
    }
}
