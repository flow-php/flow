<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Progress;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\IO\Transaction;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\TransformationError;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Values\Rows;
use SplObjectStorage;
use Throwable;

/**
 * The planner's step for a transaction root: one transaction per batch, one around the drain.
 */
final readonly class TransactionLoader implements NestedLoader
{
    /**
     * @param list<Loader|NestedLoader> $children
     * @param null|SplObjectStorage<Loader|Transformer, Progress> $stages the stage the planner built for each write
     * @param null|SplObjectStorage<Loader, Progress> $writes the write stage the planner built for each child
     */
    public function __construct(
        private Transaction $transaction,
        private array $children,
        private FlowContext $context,
        private ?SplObjectStorage $stages = null,
        private ?SplObjectStorage $writes = null,
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
                    if ($this->writes !== null && $this->writes->offsetExists($child) && $rows->count() > 0) {
                        $this->writes->offsetGet($child)->advanced($rows->count());
                    }

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
    public function closure(Progress $progress): void
    {
        $this->endWrites();
        $this->transaction->begin();

        try {
            foreach ($this->children as $child) {
                if ($child instanceof Closure) {
                    $child->closure(
                        $child instanceof Loader && $this->stages !== null && $this->stages->offsetExists($child)
                            ? $this->stages->offsetGet($child)
                            : $progress,
                    );
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
        $this->endWrites();

        foreach ($this->children as $child) {
            if ($child instanceof Discardable) {
                $child->discard();
            }
        }
    }

    private function endWrites(): void
    {
        foreach ($this->children as $child) {
            if ($child instanceof Loader && $this->writes !== null && $this->writes->offsetExists($child)) {
                $this->writes->offsetGet($child)->done();
            }
        }
    }
}
