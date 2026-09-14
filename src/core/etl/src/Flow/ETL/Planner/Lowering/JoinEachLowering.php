<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\JoinEach;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\JoinEachRowsTransformer;

/**
 * @implements Lowering<JoinEach>
 */
final readonly class JoinEachLowering implements Lowering
{
    /**
     * @return class-string<JoinEach>
     */
    public function handles(): string
    {
        return JoinEach::class;
    }

    /**
     * @param JoinEach $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return match ($node->type) {
            JoinType::left => [JoinEachRowsTransformer::left($node->factory, $node->on)],
            JoinType::left_anti => [JoinEachRowsTransformer::leftAnti($node->factory, $node->on)],
            JoinType::right => [JoinEachRowsTransformer::right($node->factory, $node->on)],
            JoinType::inner => [JoinEachRowsTransformer::inner($node->factory, $node->on)],
        };
    }
}
