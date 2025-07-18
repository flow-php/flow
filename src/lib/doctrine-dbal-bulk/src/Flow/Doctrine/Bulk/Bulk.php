<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\{Connection, Exception};
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Doctrine\Bulk\QueryFactory\DbalQueryFactory;

final readonly class Bulk
{
    public function __construct(private QueryFactory $queryFactory, private TableDefinitions $tableDefinitions)
    {
    }

    public static function create() : self
    {
        return new self(new DbalQueryFactory(), new TableDefinitions());
    }

    /**
     * Delete data from the database in bulk based on the provided bulk data.
     *
     * @throws Exception|RuntimeException
     */
    public function delete(Connection $connection, string $table, BulkData $bulkData) : void
    {
        $tableDefinition = $this->tableDefinitions->get($table, $connection);

        $connection->executeStatement(
            $this->queryFactory->delete($connection->getDatabasePlatform(), $tableDefinition, $bulkData),
            $bulkData->toSqlParameters($tableDefinition),
            $tableDefinition->dbalTypes($bulkData)
        );
    }

    /**
     * Insert data into the database in bulk.
     * Insert should be used whenever you want to insert a large number of rows into a table or upsert them.
     *
     * Each database platform has its own way of handling bulk inserts, please make sure to use the correct implementation of InsertOptions for your platform.
     *
     * @throws Exception|RuntimeException
     */
    public function insert(Connection $connection, string $table, BulkData $bulkData, ?InsertOptions $options = null) : void
    {
        $tableDefinition = $this->tableDefinitions->get($table, $connection);

        $connection->executeStatement(
            $this->queryFactory->insert($connection->getDatabasePlatform(), $tableDefinition, $bulkData, $options),
            $bulkData->toSqlParameters($tableDefinition),
            $bulkData->types()
        );
    }

    /**
     * @param Connection $connection
     * @param string $table
     * @param BulkData $bulkData
     *
     * @throws Exception|RuntimeException
     */
    public function update(Connection $connection, string $table, BulkData $bulkData, ?UpdateOptions $options = null) : void
    {
        $tableDefinition = $this->tableDefinitions->get($table, $connection);

        $connection->executeStatement(
            $this->queryFactory->update($connection->getDatabasePlatform(), $tableDefinition, $bulkData, $options),
            $bulkData->toSqlParameters($tableDefinition),
            $bulkData->types()
        );
    }
}
