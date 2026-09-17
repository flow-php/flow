<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer;

use Flow\ETL\Config\Sort\ExternalSortConfig;
use Flow\ETL\FlowContext;
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
    public function __construct(
        private FlowContext $context,
    ) {}

    public function of(Node $node): Node
    {
        if (!$node instanceof Limit) {
            return $node;
        }

        $sort = $node->children()[0];

        if (!$sort instanceof Sort) {
            return $node;
        }

        $config = $this->context->config;
        $algorithm = $sort->algorithm?->build($config->cache->localFilesystemCacheDir) ?? $config->sort;

        if ($algorithm instanceof ExternalSortConfig && $node->limit > $algorithm->runSize) {
            return $node;
        }

        return new TopN($sort->children()[0], $sort->refs, $node->limit);
    }
}
