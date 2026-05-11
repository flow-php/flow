<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * Processor handles cross-batch operations that need to see multiple/all batches.
 *
 * Unlike Transformer which operates on a single batch of Rows, a Processor receives
 * the entire upstream generator and produces a new generator. This allows operations
 * like sorting, grouping, and batching that need to accumulate data across batches.
 *
 * @internal
 */
interface Processor
{
    /**
     * Process a stream of Rows and return a new stream.
     *
     * @param \Generator<Rows> $rows
     *
     * @return \Generator<Rows>
     */
    public function process(\Generator $rows, FlowContext $context): \Generator;
}
