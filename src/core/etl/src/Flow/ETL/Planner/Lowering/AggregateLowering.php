<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy\GroupBySteps;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Aggregate;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Aggregate>
 */
final readonly class AggregateLowering implements Lowering
{
    /**
     * @return class-string<Aggregate>
     */
    public function handles(): string
    {
        return Aggregate::class;
    }

    /**
     * @param Aggregate $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return GroupBySteps::of($node->groupBy, $context->config, $node->algorithm);
    }
}
