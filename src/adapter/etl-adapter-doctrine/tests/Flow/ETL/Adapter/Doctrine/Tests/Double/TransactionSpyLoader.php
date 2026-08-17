<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Double;

use Doctrine\DBAL\Connection;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Rows;

final class TransactionSpyLoader implements Closure, Loader
{
    /** @var list<bool> */
    public array $closureInTransaction = [];

    /** @var list<array{rows: int, inTransaction: bool}> */
    public array $deliveries = [];

    public function __construct(
        private readonly Connection $connection,
    ) {}

    public function closure(FlowContext $context): void
    {
        $this->closureInTransaction[] = $this->connection->isTransactionActive();
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->deliveries[] = ['rows' => $rows->count(), 'inTransaction' => $this->connection->isTransactionActive()];
    }
}
