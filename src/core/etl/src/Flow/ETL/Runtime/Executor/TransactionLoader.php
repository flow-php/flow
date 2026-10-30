<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\IO\Transaction;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\TransformationError;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Values\Rows;
use Throwable;

/**
 * The planner's step for a transaction root: one transaction per batch, one around the drain.
 */
final readonly class TransactionLoader implements NestedLoader
{
    /**
     * @param list<Loader|NestedLoader> $children
     */
    public function __construct(
        private Transaction $transaction,
        private array $children,
        private FlowContext $context,
    ) {}

    /**
     * Rolls back on any failure. A failure no user step raised - begin, commit, an unoffered branch failure - surfaces
     * as it is.
     */
    public function load(Rows $rows): LoadingError|TransformationError|null
    {
        $this->transaction->begin();
        $error = null;
        $failure = null;

        foreach ($this->children as $child) {
            try {
                if ($child instanceof Loader) {
                    (new StepTelemetry())->load($child, $rows, $this->context);
                } else {
                    $error = $child->load($rows);
                }
            } catch (Throwable $failure) {
                $error = $child instanceof Loader ? new LoadingError($failure, $child, $rows) : null;
            }

            $failure = $error->cause ?? $failure;

            if ($failure !== null) {
                break;
            }
        }

        if ($failure === null) {
            try {
                $this->transaction->commit();

                return null;
            } catch (Throwable $failure) {
                // the commit failed: rolled back below, like a failing write
            }
        }

        // the failure is the actionable one, so a rollback that fails too is logged and never masks it
        try {
            $this->transaction->rollback($failure);
        } catch (Throwable $rollback) {
            $this->context
                ->telemetry()
                ->logger()
                ->error('Transaction failed to roll back.', ['exception' => $rollback]);
        }

        return $error ?? throw $failure;
    }

    /**
     * The endings path offers nothing, so nothing is wrapped: a failing drain surfaces as the user's own exception.
     */
    public function closure(): void
    {
        $this->transaction->begin();

        try {
            foreach ($this->children as $child) {
                if ($child instanceof Closure) {
                    $child->closure();
                }
            }

            $this->transaction->commit();
        } catch (Throwable $failure) {
            try {
                $this->transaction->rollback($failure);
            } catch (Throwable $rollback) {
                $this->context
                    ->telemetry()
                    ->logger()
                    ->error('Transaction failed to roll back.', ['exception' => $rollback]);
            }

            throw $failure;
        }
    }

    /**
     * No transaction: nothing is written on a dead run.
     */
    public function discard(): void
    {
        foreach ($this->children as $child) {
            if ($child instanceof Discardable) {
                $child->discard();
            }
        }
    }
}
