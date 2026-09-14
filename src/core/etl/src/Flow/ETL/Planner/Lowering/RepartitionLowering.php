<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Repartition;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Repartition\RepartitionSteps;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Repartition>
 */
final readonly class RepartitionLowering implements Lowering
{
    /**
     * @return class-string<Repartition>
     */
    public function handles(): string
    {
        return Repartition::class;
    }

    /**
     * @param Repartition $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return RepartitionSteps::of($node->by, $context->config);
    }
}
