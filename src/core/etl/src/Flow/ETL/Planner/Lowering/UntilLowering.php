<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Until;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\UntilTransformer;

/**
 * @implements Lowering<Until>
 */
final readonly class UntilLowering implements Lowering
{
    /**
     * @return class-string<Until>
     */
    public function handles(): string
    {
        return Until::class;
    }

    /**
     * @param Until $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new UntilTransformer($node->function)];
    }
}
