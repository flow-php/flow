<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Constrain;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Processor\ConstrainedProcessor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Constrain>
 */
final readonly class ConstrainLowering implements Lowering
{
    /**
     * @return class-string<Constrain>
     */
    public function handles(): string
    {
        return Constrain::class;
    }

    /**
     * @param Constrain $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new ConstrainedProcessor($node->constraints)];
    }
}
