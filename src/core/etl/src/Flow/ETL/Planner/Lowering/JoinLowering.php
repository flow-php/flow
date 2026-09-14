<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Join\JoinSteps;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Join;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Join>
 */
final readonly class JoinLowering implements Lowering
{
    /**
     * @return class-string<Join>
     */
    public function handles(): string
    {
        return Join::class;
    }

    /**
     * @param Join $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return JoinSteps::of($frames[0], $node->on, $node->type, $context->config, $node->algorithm);
    }
}
