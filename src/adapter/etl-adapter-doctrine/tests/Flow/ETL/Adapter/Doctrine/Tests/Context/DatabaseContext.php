<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Table;

final class DatabaseContext
{
    public function __construct(
        private readonly Connection $connection,
        private readonly InsertQueryCounter $logger
    ) {
    }

    public function connection() : Connection
    {
        return $this->connection;
    }

    public function createTable(Table $table) : void
    {
        $schemaManager = $this->connection->createSchemaManager();

        if ($schemaManager->tablesExist([$table->getName()])) {
            $schemaManager->dropTable($table->getName());
        }

        $schemaManager->createTable($table);
    }

    public function dropAllTables() : void
    {
        $schemaManager = $this->connection->createSchemaManager();

        foreach ($schemaManager->listTables() as $table) {
            $schemaManager->dropTable($table->getName());
        }
    }

    public function insert(string $tableName, array $data, array $types = []) : void
    {
        $this->connection->insert($tableName, $data, $types);
    }

    public function numberOfExecutedInsertQueries() : int
    {
        return $this->logger->count;
    }

    public function selectAll(string $tableName) : array
    {
        return $this->connection->fetchAllAssociative(
            $this
                ->connection
                ->createQueryBuilder()
                ->select('*')
                ->from($tableName)
                ->orderBy('id')
                ->getSQL()
        );
    }

    public function tableCount(string $tableName) : int
    {
        return (int) $this->connection->fetchOne(
            $this
                ->connection
                ->createQueryBuilder()
                ->select('COUNT(*)')
                ->from($tableName)
                ->getSQL()
        );
    }
}
