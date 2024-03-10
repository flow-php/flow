<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\{Connection, Exception};
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Doctrine\Bulk\QueryFactory\DbalQueryFactory;

final class Bulk
{
    private QueryFactory $queryFactory;

    public function __construct(QueryFactory $queryFactory)
    {
        $this->queryFactory = $queryFactory;
    }

    public static function create() : self
    {
        return new self(new DbalQueryFactory());
    }

    /**
     * @param Connection $connection
     * @param string $table
     * @param BulkData $bulkData
     * @param array{
     *  skip_conflicts?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>
     * } $insertOptions $insertOptions
     *
     * @throws Exception|RuntimeException
     */
    public function insert(Connection $connection, string $table, BulkData $bulkData, array $insertOptions = []) : void
    {
        $tableDefinition = new TableDefinition($table, ...\array_values($connection->createSchemaManager()->listTableColumns($table)));

        $connection->executeStatement(
            $this->queryFactory->insert($connection->getDatabasePlatform(), $tableDefinition, $bulkData, $insertOptions),
            $bulkData->toSqlParameters($tableDefinition),
            $tableDefinition->dbalTypes($bulkData)
        );
    }

    /**
     * @param Connection $connection
     * @param string $table
     * @param BulkData $bulkData
     * @param array{
     *  primary_key_columns?: array<string>,
     *  update_columns?: array<string>
     * } $updateOptions $updateOptions
     *
     * @throws Exception|RuntimeException
     */
    public function update(Connection $connection, string $table, BulkData $bulkData, array $updateOptions = []) : void
    {
        $tableDefinition = new TableDefinition($table, ...\array_values($connection->createSchemaManager()->listTableColumns($table)));

        $connection->executeQuery(
            $this->queryFactory->update($connection->getDatabasePlatform(), $tableDefinition, $bulkData, $updateOptions),
            $bulkData->toSqlParameters($tableDefinition),
            $tableDefinition->dbalTypes($bulkData)
        );
    }
}
