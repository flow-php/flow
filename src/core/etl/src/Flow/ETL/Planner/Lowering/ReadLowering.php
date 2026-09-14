<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Read>
 */
final readonly class ReadLowering implements Lowering
{
    /**
     * @return class-string<Read>
     */
    public function handles(): string
    {
        return Read::class;
    }

    /**
     * @param Read $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [];
    }
}
