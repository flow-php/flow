<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Drop;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\DropEntriesTransformer;

/**
 * @implements Lowering<Drop>
 */
final readonly class DropLowering implements Lowering
{
    /**
     * @return class-string<Drop>
     */
    public function handles(): string
    {
        return Drop::class;
    }

    /**
     * @param Drop $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new DropEntriesTransformer(...$node->entries)];
    }
}
