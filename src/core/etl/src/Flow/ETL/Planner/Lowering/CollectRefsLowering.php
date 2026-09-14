<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\CollectRefs;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\CollectReferencesTransformer;

/**
 * @implements Lowering<CollectRefs>
 */
final readonly class CollectRefsLowering implements Lowering
{
    /**
     * @return class-string<CollectRefs>
     */
    public function handles(): string
    {
        return CollectRefs::class;
    }

    /**
     * @param CollectRefs $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new CollectReferencesTransformer($node->references)];
    }
}
