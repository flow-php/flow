<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\TransactionIsolationLevel;
use Flow\ETL\Transaction;
use Throwable;

/**
 * @import-type Params from DriverManager
 */
final class DbalTransaction implements Transaction
{
    private ?Connection $connection = null;

    /**
     * @var Params
     */
    private readonly array $connectionParams;

    private ?TransactionIsolationLevel $isolationLevel = null;

    private ?IsolationLevelRestore $restore = null;

    /**
     * @param array<string, mixed> $connectionParams
     */
    public function __construct(array $connectionParams)
    {
        /** @var Params $connectionParams */
        $this->connectionParams = $connectionParams;
    }

    /**
     * Since Connection::getParams() is marked as an internal method, please use this constructor with caution.
     */
    public static function fromConnection(Connection $connection): self
    {
        $transaction = new self($connection->getParams());
        $transaction->connection = $connection;

        return $transaction;
    }

    /**
     * The previous level is restored when the transaction ends, so the user's Connection is never left changed.
     */
    public function withIsolationLevel(TransactionIsolationLevel $level): self
    {
        $transaction = new self($this->connectionParams);
        $transaction->connection = $this->connection;
        $transaction->isolationLevel = $level;

        return $transaction;
    }

    public function begin(): void
    {
        $connection = $this->connection ??= DriverManager::getConnection($this->connectionParams);

        if ($this->isolationLevel !== null) {
            $this->restore = new IsolationLevelRestore($connection, $connection->getTransactionIsolation());
            $connection->setTransactionIsolation($this->isolationLevel);
        }

        try {
            $connection->beginTransaction();
        } catch (Throwable $failure) {
            $this->restore?->restore();
            $this->restore = null;

            throw $failure;
        }
    }

    /**
     * A failed commit keeps the changed level: the transaction is still open, so the rollback() that follows restores it.
     */
    public function commit(): void
    {
        ($this->connection ??= DriverManager::getConnection($this->connectionParams))->commit();

        $this->restore?->restore();
        $this->restore = null;
    }

    public function rollback(Throwable $cause): void
    {
        try {
            ($this->connection ??= DriverManager::getConnection($this->connectionParams))->rollBack();
        } catch (Throwable) {
            // $cause is the actionable failure - a rollback failure must not mask it
        } finally {
            $this->restore?->restore();
            $this->restore = null;
        }
    }
}
