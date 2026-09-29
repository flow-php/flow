<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer;

use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Node\TopN;
use Flow\ETL\Plan\Rewrite;

/**
 * Replaces a Limit directly over a Sort with a TopN; see CombineSortAndLimit.
 */
final readonly class TopNRewrite implements Rewrite
{
    public function of(Node $node): Node
    {
        if (!$node instanceof Limit) {
            return $node;
        }

        $sort = $node->children()[0];

        if (!$sort instanceof Sort) {
            return $node;
        }

        // a TopN past its memory limit falls back to the sort's own spilling path, so any limit is safe to rewrite
        return new TopN($sort->children()[0], $sort->refs, $node->limit, $sort->algorithm);
    }
}
