<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\BatchBy;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Processor\BatchingByProcessor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<BatchBy>
 */
final readonly class BatchByLowering implements Lowering
{
    /**
     * @return class-string<BatchBy>
     */
    public function handles(): string
    {
        return BatchBy::class;
    }

    /**
     * @param BatchBy $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new BatchingByProcessor($node->column, $node->minSize)];
    }
}
