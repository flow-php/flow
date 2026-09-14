<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Transaction;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;
use Throwable;

use function Flow\PostgreSql\DSL\set_transaction;

final class PostgreSqlTransaction implements Transaction
{
    private ?IsolationLevel $isolationLevel = null;

    public function __construct(
        private readonly Client $client,
    ) {}

    /**
     * SET TRANSACTION applies to the open transaction only, so there is nothing to restore.
     */
    public function withIsolationLevel(IsolationLevel $level): self
    {
        $transaction = new self($this->client);
        $transaction->isolationLevel = $level;

        return $transaction;
    }

    public function begin(): void
    {
        $this->client->beginTransaction();

        if ($this->isolationLevel === null) {
            return;
        }

        try {
            $this->client->execute(set_transaction()->isolationLevel($this->isolationLevel));
        } catch (Throwable $failure) {
            // a failed begin() is never rolled back by its caller, so the transaction it opened is closed here
            $this->rollback($failure);

            throw $failure;
        }
    }

    public function commit(): void
    {
        $this->client->commit();
    }

    public function rollback(Throwable $cause): void
    {
        try {
            $this->client->rollBack();
        } catch (Throwable) {
            // $cause is the actionable failure - a rollback failure must not mask it
        }
    }
}
