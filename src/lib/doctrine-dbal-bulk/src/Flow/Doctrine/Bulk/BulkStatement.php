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
    ) {}

    /**
     * @throws Exception|RuntimeException
     */
    public function delete(Connection $connection, TableDefinition $table, BulkData $bulkData): void
    {
        $connection->executeStatement(
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
        $connection->executeStatement(
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
        $connection->executeStatement(
            $this->queryFactory->update($connection->getDatabasePlatform(), $table, $bulkData, $options),
            $bulkData->toSqlParameters($table),
            $bulkData->types(),
        );
    }
}
