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
 * Execute multiple loaders within a single PostgreSQL transaction.
 *
 * Each batch of rows is processed in its own transaction. If any loader
 * fails, the entire batch is rolled back.
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
     * Closing happens outside the transaction, each wrapped loader publishes its own destination.
     */
    public function closure(FlowContext $context): void
    {
        foreach ($this->loaders as $loader) {
            if ($loader instanceof Closure) {
                $loader->closure($context);
            }
        }
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if ($rows->count() === 0) {
            return;
        }

        $context->telemetry()->loadingStarted($this);

        try {
            $this->executeInTransaction($rows, $context);

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

    private function executeInTransaction(Rows $rows, FlowContext $context): void
    {
        $this->client->beginTransaction();

        try {
            if ($this->isolationLevel !== null) {
                $this->client->execute(set_transaction()->isolationLevel($this->isolationLevel));
            }

            foreach ($this->loaders as $loader) {
                $loader->load($rows, $context);
            }

            $this->client->commit();
        } catch (Throwable $e) {
            $this->client->rollBack();

            throw $e;
        }
    }
}
