<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Batch;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Batch>
 */
final readonly class BatchLowering implements Lowering
{
    /**
     * @return class-string<Batch>
     */
    public function handles(): string
    {
        return Batch::class;
    }

    /**
     * @param Batch $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new BatchingProcessor($node->size)];
    }
}
