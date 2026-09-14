<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\RenameEach;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\RenameEachEntryTransformer;

/**
 * @implements Lowering<RenameEach>
 */
final readonly class RenameEachLowering implements Lowering
{
    /**
     * @return class-string<RenameEach>
     */
    public function handles(): string
    {
        return RenameEach::class;
    }

    /**
     * @param RenameEach $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new RenameEachEntryTransformer(...$node->strategies)];
    }
}
