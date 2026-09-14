<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Write>
 */
final readonly class WriteLowering implements Lowering
{
    /**
     * @return class-string<Write>
     */
    public function handles(): string
    {
        return Write::class;
    }

    /**
     * @param Write $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [$node->loader];
    }
}
