<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\LimitTransformer;

/**
 * @implements Lowering<Limit>
 */
final readonly class LimitLowering implements Lowering
{
    /**
     * @return class-string<Limit>
     */
    public function handles(): string
    {
        return Limit::class;
    }

    /**
     * @param Limit $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new LimitTransformer($node->limit)];
    }
}
