<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

/**
 * A Transaction owns no step of its own: SinkAttachment builds its TransactionalSinks from the children's steps.
 *
 * @implements Lowering<Transaction>
 */
final readonly class TransactionLowering implements Lowering
{
    /**
     * @return class-string<Transaction>
     */
    public function handles(): string
    {
        return Transaction::class;
    }

    /**
     * @param Transaction $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [];
    }
}
