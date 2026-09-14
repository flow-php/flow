<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Result>
 */
final readonly class ResultLowering implements Lowering
{
    /**
     * @return class-string<Result>
     */
    public function handles(): string
    {
        return Result::class;
    }

    /**
     * @param Result $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [];
    }
}
