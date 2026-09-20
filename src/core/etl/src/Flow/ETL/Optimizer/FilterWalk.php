<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer;

use Flow\ETL\Function\ReferencedColumns;
use Flow\ETL\Function\ReferenceRename;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;

final readonly class FilterWalk
{
    /**
     * Every consumer must read through $filter. Otherwise pushing its predicate into the source narrows what
     * another consumer reads - the filter analogue of PushLimitIntoSource's every-consumer agreement.
     *
     * @param Node ...$consumerInputs LogicalPlan::consumerInputs()
     */
    public function reachedByEveryConsumer(Node\Filter $filter, Node ...$consumerInputs): bool
    {
        foreach ($consumerInputs as $input) {
            for ($node = $input; $node !== $filter; $node = $node->children()[0] ?? null) {
                if ($node === null) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * $predicate as $leaf sees it, or null when it may not be evaluated there instead of above $filter.
     * Another Filter is TRANSPARENT here: it only removes rows, and the pushed Filter node stays, so pruning a
     * file drops rows that filter would have dropped anyway. Limit/Offset/Until/Distinct/Discard are not: they
     * make WHICH rows arrive depend on what the source read.
     *
     * A node that redefines a column the predicate reads blocks it, unless the column is a plain alias of a
     * column below (Rename, a WithColumn of a bare reference): the predicate is rewritten to read that column.
     */
    public function predicateAtLeaf(Node\Filter $filter, Read $leaf, ScalarFunction $predicate): ?ScalarFunction
    {
        $columns = new ReferencedColumns();

        for ($node = $filter->children()[0]; $node !== $leaf; $node = $node->children()[0]) {
            if ($node->children() === [] || $node->transparency() !== Transparency::transparent) {
                return null;
            }

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
