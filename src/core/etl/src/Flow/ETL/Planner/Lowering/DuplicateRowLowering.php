<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\DuplicateRow;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\DuplicateRowTransformer;

/**
 * @implements Lowering<DuplicateRow>
 */
final readonly class DuplicateRowLowering implements Lowering
{
    /**
     * @return class-string<DuplicateRow>
     */
    public function handles(): string
    {
        return DuplicateRow::class;
    }

    /**
     * @param DuplicateRow $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new DuplicateRowTransformer($node->condition, ...$node->entries)];
    }
}
