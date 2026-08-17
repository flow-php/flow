<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\OverridingLoader;
use Flow\ETL\Rows;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;
use Throwable;

use function count;
use function Flow\PostgreSql\DSL\set_transaction;

/**
 * Execute multiple loaders within PostgreSQL transactions.
 *
 * Each batch of rows is loaded in its own transaction; rows a wrapped Transformation delivers when
 * the loader is closed (blocking operations drain there) are committed in one final transaction.
 * If any loader fails, the open transaction is rolled back.
 * All wrapped loaders must use the same Client instance as the wrapper - a loader holding its own
 * Client escapes the transaction.
 */
final class TransactionalPostgreSqlLoader implements Closure, Loader, OverridingLoader
{
    private ?IsolationLevel $isolationLevel = null;

    /**
     * @var array<Loader>
     */
    private readonly array $loaders;

    public function __construct(
        private readonly Client $client,
        Loader ...$loaders,
    ) {
        if (count($loaders) === 0) {
            throw new InvalidArgumentException('At least one loader must be provided');
        }

        $this->loaders = $loaders;
    }

    /**
     * Rows a wrapped Transformation buffered (blocking operations - sortBy, aggregate, groupBy->aggregate,
     * pivot, window functions, collect, join) are delivered during the forwarded closure() drain, so delivery
     * here must be transactional too: one transaction over everything the drain flushes, rolled back when it fails.
     */
    public function closure(FlowContext $context): void
    {
        $this->inTransaction(function () use ($context): void {
            foreach ($this->loaders as $loader) {
                if ($loader instanceof Closure) {
                    $loader->closure($context);
                }
            }
        });
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if ($rows->count() === 0) {
            return;
        }

        $context->telemetry()->loadingStarted($this);

        try {
            $this->inTransaction(function () use ($rows, $context): void {
                foreach ($this->loaders as $loader) {
                    $loader->load($rows, $context);
                }
            });

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function loaders(): array
    {
        return $this->loaders;
    }

    public function withIsolationLevel(IsolationLevel $level): self
    {
        $this->isolationLevel = $level;

        return $this;
    }

    /**
     * @param callable(): void $operation
     */
    private function inTransaction(callable $operation): void
    {
        $this->client->beginTransaction();

        try {
            if ($this->isolationLevel !== null) {
                $this->client->execute(set_transaction()->isolationLevel($this->isolationLevel));
            }

            $operation();

            $this->client->commit();
        } catch (Throwable $e) {
            try {
                $this->client->rollBack();
            } catch (Throwable) {
                // the load/drain failure is the actionable error - a rollback failure must not mask it
            }

            throw $e;
        }
    }
}
