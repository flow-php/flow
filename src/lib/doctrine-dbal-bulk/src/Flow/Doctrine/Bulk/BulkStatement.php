<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Flow\Doctrine\Bulk\Exception\RuntimeException;

final readonly class BulkStatement
{
    public function __construct(
        private QueryFactory $queryFactory,
        private LastPreparedStatement $statement = new LastPreparedStatement(),
    ) {}

    /**
     * @throws Exception|RuntimeException
     */
    public function delete(Connection $connection, TableDefinition $table, BulkData $bulkData): void
    {
        $this->statement->execute(
            $connection,
            $this->queryFactory->delete($connection->getDatabasePlatform(), $table, $bulkData),
            $bulkData->toSqlParameters($table),
            $table->dbalParameterTypes($bulkData),
        );
    }

    /**
     * @throws Exception|RuntimeException
     */
    public function insert(
        Connection $connection,
        TableDefinition $table,
        BulkData $bulkData,
        ?InsertOptions $options = null,
    ): void {
        $this->statement->execute(
            $connection,
            $this->queryFactory->insert($connection->getDatabasePlatform(), $table, $bulkData, $options),
            $bulkData->toSqlParameters($table),
            $bulkData->types(),
        );
    }

    /**
     * @throws Exception|RuntimeException
     */
    public function update(
        Connection $connection,
        TableDefinition $table,
        BulkData $bulkData,
        ?UpdateOptions $options = null,
    ): void {
        $this->statement->execute(
            $connection,
            $this->queryFactory->update($connection->getDatabasePlatform(), $table, $bulkData, $options),
            $bulkData->toSqlParameters($table),
            $bulkData->types(),
        );
    }
}
