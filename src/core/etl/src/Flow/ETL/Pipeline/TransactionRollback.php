<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\FlowContext;
use Flow\ETL\Transaction;
use Throwable;

/**
 * @internal
 */
final readonly class TransactionRollback
{
    public function __construct(
        private Transaction $transaction,
    ) {}

    /**
     * $cause is the actionable failure, so a rollback that fails too is logged and never masks it.
     */
    public function rollback(Throwable $cause, FlowContext $context): void
    {
        try {
            $this->transaction->rollback($cause);
        } catch (Throwable $rollback) {
            $context->telemetry()->logger()->error('Transaction failed to roll back.', ['exception' => $rollback]);
        }
    }
}
