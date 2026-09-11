<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\TransactionIsolationLevel;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\OverridingLoader;
use Flow\ETL\Rows;
use Throwable;

use function count;

/**
 * @import-type Params from DriverManager
 */
final class TransactionalDbalLoader implements Closure, Loader, OverridingLoader
{
    private ?Connection $connection = null;

    private ?TransactionIsolationLevel $isolationLevel = null;

    /**
     * @var array<Loader>
     */
    private readonly array $loaders;

    /**
     * @param array<string, mixed> $connectionParams
     * @param Loader ...$loaders
     */
    public function __construct(
        private readonly array $connectionParams,
        Loader ...$loaders,
    ) {
        if (count($loaders) === 0) {
            throw new InvalidArgumentException('At least one loader must be provided');
        }

        $this->loaders = $loaders;
    }

    /**
     * Since Connection::getParams() is marked as an internal method, please
     * use this constructor with caution.
     */
    public static function fromConnection(Connection $connection, Loader ...$loaders): self
    {
        $loader = new self($connection->getParams(), ...$loaders);
        $loader->connection = $connection;

        return $loader;
    }

    /**
     * Rows a wrapped Transformation buffered (blocking operations - sortBy, aggregate, groupBy->aggregate,
     * pivot, window functions, collect, join) are delivered during the forwarded closure() drain, so delivery
     * here must be transactional too: one transaction over everything the drain flushes, rolled back when it fails.
     */
    public function closure(FlowContext $context): void
    {
        $this->inTransaction($this->connection(), function () use ($context): void {
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
            $this->inTransaction($this->connection(), function () use ($rows, $context): void {
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

    public function withIsolationLevel(TransactionIsolationLevel $level): self
    {
        $this->isolationLevel = $level;

        return $this;
    }

    private function connection(): Connection
    {
        if ($this->connection === null) {
            /** @var Params $connectionParams */
            $connectionParams = $this->connectionParams;
            $this->connection = DriverManager::getConnection($connectionParams);
        }

        return $this->connection;
    }

    /**
     * @param callable(): void $operation
     */
    private function inTransaction(Connection $connection, callable $operation): void
    {
        $previousIsolationLevel = null;

        if ($this->isolationLevel !== null) {
            $previousIsolationLevel = $connection->getTransactionIsolation();
            $connection->setTransactionIsolation($this->isolationLevel);
        }

        try {
            $connection->beginTransaction();

            try {
                $operation();

                $connection->commit();
            } catch (Throwable $e) {
                try {
                    $connection->rollBack();
                } catch (Throwable) {
                    // the load/drain failure is the actionable error - a rollback failure must not mask it
                }

                throw $e;
            }
        } finally {
            if ($previousIsolationLevel !== null) {
                try {
                    $connection->setTransactionIsolation($previousIsolationLevel);
                } catch (Throwable) {
                    // restoring connection state must not mask an in-flight failure
                }
            }
        }
    }
}
