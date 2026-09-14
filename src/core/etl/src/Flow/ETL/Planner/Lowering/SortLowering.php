<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Sort\SortSteps;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Sort>
 */
final readonly class SortLowering implements Lowering
{
    /**
     * @return class-string<Sort>
     */
    public function handles(): string
    {
        return Sort::class;
    }

    /**
     * @param Sort $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return SortSteps::of($node->refs, $context->config, $node->algorithm);
    }
}
