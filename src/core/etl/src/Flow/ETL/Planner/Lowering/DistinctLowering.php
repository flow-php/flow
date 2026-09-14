<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Distinct;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\DropDuplicatesTransformer;

/**
 * @implements Lowering<Distinct>
 */
final readonly class DistinctLowering implements Lowering
{
    /**
     * @return class-string<Distinct>
     */
    public function handles(): string
    {
        return Distinct::class;
    }

    /**
     * @param Distinct $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new DropDuplicatesTransformer(...$node->entries)];
    }
}
