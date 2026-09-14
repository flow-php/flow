<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\FrameOutput;
use Flow\ETL\Plan\Node;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

/**
 * @template TNode of Node
 */
interface Lowering
{
    /**
     * @return class-string<TNode>
     */
    public function handles(): string;

    /**
     * Called once per run, so a step Flow constructs here is rebuilt every run and a second run never sees the state
     * the first left behind. An instance the USER constructed - a Loader, or a Transformer handed to
     * transform()/rows() - is theirs: hand it back as it is, never reset it.
     *
     * @param TNode $node PHP's LSP forces the native type to stay Node; the template narrows it
     * @param list<FrameOutput> $frames the physical output of this node's side inputs (children() beyond
     *                                  the first), in order; [] for a node with no side input
     *
     * @return list<Loader|Processor|Transformer> in execution order; [] for a node that only sources rows
     */
    public function steps(Node $node, FlowContext $context, array $frames): array;
}
