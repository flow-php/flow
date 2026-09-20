<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\TransactionIsolationLevel;
use Throwable;

/**
 * The isolation level a DbalTransaction changed, and how to put it back.
 */
final readonly class IsolationLevelRestore
{
    public function __construct(
        private Connection $connection,
        private TransactionIsolationLevel $previous,
    ) {}

    public function restore(): void
    {
        try {
            $this->connection->setTransactionIsolation($this->previous);
        } catch (Throwable) {
            // restoring connection state must not mask an in-flight failure
        }
    }
}
