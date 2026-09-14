<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Rule;

use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Filesystem\ScalarFunctionFilter;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\All;
use Flow\ETL\Function\ReferencedColumns;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\ReplaceLeaf;
use Flow\ETL\Planner\FilterWalk;
use Flow\ETL\Planner\Rule;

use function array_diff;

/**
 * Evaluate a partition-only predicate once per FILE instead of once per row: the file never opens. The
 * Filter node STAYS (Spark PruneFileSourcePartitions "Keep partition-pruning predicates"), so the result is
 * unchanged and a source that ignores Scan::$pathFilter is still correct.
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

        $roots = $plan->consumers();
        $walk = new FilterWalk();
        $columns = new ReferencedColumns();
        $names = $partitions->references()->names();
        $read = $leaf;

        // ONE pass over the ORIGINAL tree: transformUp() rebuilds every node, so a second pass would walk
        // a tree no longer in the plan and $roots identity would never match again
        for ($node = $plan->root; $node->children() !== []; $node = $node->children()[0]) {
            if (!$node instanceof Node\Filter || !$walk->reachedByEveryRoot($node, ...$roots)) {
                continue;
            }

            // Spark DataSourceUtils::getPartitionFiltersAndDataFilters / Polars MintermIter: from a mixed
            // AND, the partition-only conjuncts prune and the Filter keeps the whole predicate
            foreach ($node->function instanceof All ? $node->function->children() : [$node->function] as $conjunct) {
                if (!$conjunct->deterministic()) {
                    continue;
                }

                $refs = $columns->in($conjunct);

                if ($refs->all() === [] || array_diff($refs->names(), $names) !== []) {
                    continue;
                }

                if (!$walk->reaches($node, $leaf, $refs)) {
                    continue;
                }

                $read = $read->withPathFilter(new ScalarFunctionFilter($conjunct, $partitions, $context));
            }
        }

        return $read === $leaf ? $plan : $plan->transformUp(new ReplaceLeaf($leaf, $read));
    }
}
