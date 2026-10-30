<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Optimizer\Rule;

use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Limit;
use Flow\ETL\Planning\Logical\Node\Offset;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\ReplaceNodes;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Values\Column\Backend;

use function max;
use function min;

final readonly class PushLimitIntoSource implements Rule
{
    /**
     * One walk per consumer, pushed only when EVERY walk reaches the source: a limit collected from one consumer
     * must not narrow what another consumer of the same source reads, so the widest one is pushed.
     */
    public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan
    {
        $leaf = $plan->source();
        $limits = [];

        foreach ($plan->consumerInputs() as $input) {
            $limit = $this->limitAt($input, $leaf);

            if ($limit === null) {
                return $plan;
            }

            $limits[] = $limit;
        }

        return $plan->transformUp(ReplaceNodes::one($leaf, $leaf->withLimit(max($limits))));
    }

    /**
     * The limit ONE root lets through to $leaf. Limits fold with min(); a node that is not preserving or not
     * transparent DISCARDS what was collected above it and the walk continues, so
     * read->limit(5)->sort()->limit(3) still yields 5. An Offset grows the limit by the rows it skips, so
     * read->offset(100)->limit(10) yields 110.
     *
     * @return null|int null when no limit survives, and when this chain does not reach $leaf at all
     */
    public function limitAt(Node $from, Read $leaf): ?int
    {
        $limit = null;

        for ($node = $from; $node !== $leaf; $node = $node->children()[0]) {
            if ($node instanceof Limit) {
                $limit = $limit === null ? $node->limit : min($limit, $node->limit);
            } elseif ($node instanceof Offset) {
                // the source must read the skipped rows too; the Offset and Limit nodes above still cut them
                $limit = $limit === null ? null : $limit + $node->offset;
            } elseif (
                $node->rowCount() !== RowCount::preserving
                || $node->transparency() !== Transparency::transparent
            ) {
                $limit = null;
            }

            if ($node->children() === []) {
                return null;
            }
        }

        return $limit;
    }
}
