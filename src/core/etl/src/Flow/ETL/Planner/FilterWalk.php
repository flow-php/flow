<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Row\References;

final readonly class FilterWalk
{
    /**
     * Every root must consume through $filter. Otherwise pushing its predicate into the source narrows a
     * scan another root reads - the filter analogue of PushLimitIntoSource's every-root agreement.
     */
    public function reachedByEveryRoot(Node\Filter $filter, Node ...$roots): bool
    {
        foreach ($roots as $root) {
            for ($node = $root; $node !== $filter; $node = $node->children()[0] ?? null) {
                if ($node === null) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * True when $filter's predicate may be evaluated at $leaf instead of above it. Another Filter is
     * TRANSPARENT here: it only removes rows, and the pushed Filter node stays, so pruning a file drops
     * rows that filter would have dropped anyway. Limit/Offset/Until/Distinct/Discard are not: they make
     * WHICH rows arrive depend on what the source read.
     */
    public function reaches(Node\Filter $filter, Read $leaf, References $refs): bool
    {
        for ($node = $filter->children()[0]; $node !== $leaf; $node = $node->children()[0]) {
            if ($node->children() === [] || $node->transparency() !== Transparency::transparent) {
                return false;
            }

            if ($node->rowCount() === RowCount::reducing && !$node instanceof Node\Filter) {
                return false;
            }

            // a redefined name is a different column below this node; Flow has no expression ids, so
            // substitution (Spark's replaceAlias) is not available and a barrier is the sound answer
            if ($node->redefines()->overlaps($refs)) {
                return false;
            }
        }

        return true;
    }
}
