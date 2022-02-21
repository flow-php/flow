<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Connection;
use Flow\Doctrine\Bulk\QueryFactory\DbalQueryFactory;

final class BulkInsert
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
     * @throws \Doctrine\DBAL\Exception
     * @psalm-suppress DeprecatedMethod
     */
    public function insert(Connection $connection, string $table, BulkData $bulkData, array $insertOptions = []) : void
    {
        $tableDefinition = new TableDefinition($table, ...\array_values($connection->getSchemaManager()->listTableColumns($table)));

        $connection->executeStatement(
            $this->queryFactory->insert($connection->getDatabasePlatform(), $tableDefinition, $bulkData, $insertOptions),
            $bulkData->toSqlParameters(),
            $tableDefinition->dbalTypes($bulkData)
        );
    }

    /**
     * @param Connection $connection
     * @param string $table
     * @param BulkData $bulkData
     *
     * @throws \Doctrine\DBAL\Exception
     *
     *@deprecated
     */
    public function insertOrSkipOnConflict(Connection $connection, string $table, BulkData $bulkData) : void
    {
        $this->insert($connection, $table, $bulkData, [
            'skip_conflicts' => true,
        ]);
    }

    /**
     * @param Connection $connection
     * @param string $table
     * @param string $constraint
     * @param BulkData $bulkData
     *
     * @throws \Doctrine\DBAL\Exception
     *
     *@deprecated
     */
    public function insertOrUpdateOnConstraintConflict(Connection $connection, string $table, string $constraint, BulkData $bulkData) : void
    {
        $this->insert($connection, $table, $bulkData, [
            'constraint' => $constraint,
        ]);
    }

    /**
     * @param Connection $connection
     * @param string $table
     * @param array<string> $conflictColumns
     * @param BulkData $bulkData
     * @param array<string> $updateColumns
     *
     * @throws \Doctrine\DBAL\Exception
     *
     *@deprecated
     */
    public function insertOrUpdateOnConflict(Connection $connection, string $table, array $conflictColumns, BulkData $bulkData, array $updateColumns = []) : void
    {
        $this->insert($connection, $table, $bulkData, [
            'update_columns' => $updateColumns,
            'conflict_columns' => $conflictColumns,
        ]);
    }
}
