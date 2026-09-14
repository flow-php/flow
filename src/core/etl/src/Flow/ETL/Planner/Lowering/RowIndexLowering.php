<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\RowIndex;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\AddRowIndexTransformer;

/**
 * @implements Lowering<RowIndex>
 */
final readonly class RowIndexLowering implements Lowering
{
    /**
     * @return class-string<RowIndex>
     */
    public function handles(): string
    {
        return RowIndex::class;
    }

    /**
     * @param RowIndex $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new AddRowIndexTransformer($node->indexColumn, $node->startFrom)];
    }
}
