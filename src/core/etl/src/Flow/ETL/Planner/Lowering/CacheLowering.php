<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Cache;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\CachingProcessor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Cache>
 */
final readonly class CacheLowering implements Lowering
{
    /**
     * @return class-string<Cache>
     */
    public function handles(): string
    {
        return Cache::class;
    }

    /**
     * @param Cache $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return (
            $node->batchSize
                ? [new BatchingProcessor($node->batchSize), new CachingProcessor($node->id, $node->cache)]
                : [new CachingProcessor($node->id, $node->cache)]
        );
    }
}
