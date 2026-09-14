<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Offset;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Processor\OffsetProcessor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Offset>
 */
final readonly class OffsetLowering implements Lowering
{
    /**
     * @return class-string<Offset>
     */
    public function handles(): string
    {
        return Offset::class;
    }

    /**
     * @param Offset $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new OffsetProcessor($node->offset)];
    }
}
