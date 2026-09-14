<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Collect;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Collect>
 */
final readonly class CollectLowering implements Lowering
{
    /**
     * @return class-string<Collect>
     */
    public function handles(): string
    {
        return Collect::class;
    }

    /**
     * @param Collect $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new CollectingProcessor()];
    }
}
