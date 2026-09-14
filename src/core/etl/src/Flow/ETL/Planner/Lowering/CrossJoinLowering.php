<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;

/**
 * @implements Lowering<CrossJoin>
 */
final readonly class CrossJoinLowering implements Lowering
{
    /**
     * @return class-string<CrossJoin>
     */
    public function handles(): string
    {
        return CrossJoin::class;
    }

    /**
     * @param CrossJoin $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new CrossJoinRowsTransformer($frames[0], $node->prefix)];
    }
}
