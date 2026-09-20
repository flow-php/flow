<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer;

use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Offset;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;

use function min;

final readonly class LimitWalk
{
    /**
     * The limit ONE root lets through to $leaf. Limits fold with min(); a node that is not preserving or not
     * transparent DISCARDS what was collected above it and the walk continues, so
     * read->limit(5)->sort()->limit(3) still yields 5. An Offset grows the limit by the rows it skips, so
     * read->offset(100)->limit(10) yields 110.
     *
     * @return null|int null when no limit survives, and when this chain does not reach $leaf at all
     */
    public function of(Node $from, Read $leaf): ?int
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
