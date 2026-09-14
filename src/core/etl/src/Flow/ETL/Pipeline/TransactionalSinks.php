<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\SideRootFailure;
use Flow\ETL\Exception\TransactionRolledBack;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Rows;
use Flow\ETL\Transaction;
use Throwable;

/**
 * @internal the planner's step for a transaction root: one transaction per batch, one around the drain
 */
final readonly class TransactionalSinks implements Closure, Discardable, Loader
{
    private TransactionRollback $rollback;

    /**
     * @param list<Loader|SinkFeed> $children
     */
    public function __construct(
        private Transaction $transaction,
        private array $children,
    ) {
        $this->rollback = new TransactionRollback($transaction);
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->transaction->begin();

        foreach ($this->children as $child) {
            try {
                $child->load($rows, $context);
            } catch (Throwable $failure) {
                $this->rollback->rollback($failure, $context);

                // the FAILING child only: restarting a live sibling would unwind its fiber and lose what it buffered
                if ($child instanceof SinkFeed) {
                    $child->restart();
                }

                throw new TransactionRolledBack(
                    $child,
                    $failure instanceof SideRootFailure ? $failure->cause : $failure,
                );
            }
        }

        try {
            $this->transaction->commit();
        } catch (Throwable $failure) {
            $this->rollback->rollback($failure, $context);

            throw $failure;
        }
    }

    /**
     * The endings path offers nothing, so nothing is wrapped: a failing drain surfaces as the user's own exception.
     */
    public function closure(FlowContext $context): void
    {
        $this->transaction->begin();

        try {
            foreach ($this->children as $child) {
                if ($child instanceof Closure) {
                    $child->closure($context);
                }
            }

            $this->transaction->commit();
        } catch (Throwable $failure) {
            $this->rollback->rollback($failure, $context);

            throw $failure;
        }
    }

    /**
     * No transaction: nothing is written on a dead run.
     */
    public function discard(FlowContext $context): void
    {
        foreach ($this->children as $child) {
            if ($child instanceof Discardable) {
                $child->discard($context);
            }
        }
    }
}
