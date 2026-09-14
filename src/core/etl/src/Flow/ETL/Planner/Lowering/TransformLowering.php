<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Transform;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Transform>
 */
final readonly class TransformLowering implements Lowering
{
    /**
     * @return class-string<Transform>
     */
    public function handles(): string
    {
        return Transform::class;
    }

    /**
     * @param Transform $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [$node->transformer];
    }
}
