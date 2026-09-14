<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;

/**
 * @implements Lowering<Select>
 */
final readonly class SelectLowering implements Lowering
{
    /**
     * @return class-string<Select>
     */
    public function handles(): string
    {
        return Select::class;
    }

    /**
     * @param Select $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new SelectEntriesTransformer(...$node->entries)];
    }
}
