<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\SinkMultiple;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<SinkMultiple>
 */
final readonly class SinkMultipleLowering implements Lowering
{
    /**
     * @return class-string<SinkMultiple>
     */
    public function handles(): string
    {
        return SinkMultiple::class;
    }

    /**
     * @param SinkMultiple $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [];
    }
}
