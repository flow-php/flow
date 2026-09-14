<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;

/**
 * @implements Lowering<Filter>
 */
final readonly class FilterLowering implements Lowering
{
    /**
     * @return class-string<Filter>
     */
    public function handles(): string
    {
        return Filter::class;
    }

    /**
     * @param Filter $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new ScalarFunctionFilterTransformer($node->function)];
    }
}
