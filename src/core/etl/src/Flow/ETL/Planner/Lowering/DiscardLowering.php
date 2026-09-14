<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Discard;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Processor\VoidProcessor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Discard>
 */
final readonly class DiscardLowering implements Lowering
{
    /**
     * @return class-string<Discard>
     */
    public function handles(): string
    {
        return Discard::class;
    }

    /**
     * @param Discard $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new VoidProcessor()];
    }
}
