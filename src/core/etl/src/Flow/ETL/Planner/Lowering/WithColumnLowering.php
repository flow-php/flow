<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\WithColumn;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\ScalarFunctionTransformer;

/**
 * @implements Lowering<WithColumn>
 */
final readonly class WithColumnLowering implements Lowering
{
    /**
     * @return class-string<WithColumn>
     */
    public function handles(): string
    {
        return WithColumn::class;
    }

    /**
     * @param WithColumn $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new ScalarFunctionTransformer($node->entry, $node->function)];
    }
}
