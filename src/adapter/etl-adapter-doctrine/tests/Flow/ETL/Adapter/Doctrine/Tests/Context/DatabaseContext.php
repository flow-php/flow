<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Table;

final class DatabaseContext
{
    private array $createdTables = [];

    public function __construct(
        private readonly Connection $connection,
        private readonly InsertQueryCounter $insertQueryCounter,
        private readonly SelectQueryCounter $selectQueryCounter,
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
        $this->createdTables[] = $table->getName();
    }

    public function dropAllTables() : void
    {
        $schemaManager = $this->connection->createSchemaManager();

        foreach ($this->createdTables as $tableName) {
            $schemaManager->dropTable($tableName);
        }
    }

    public function executedSelectQueries() : array
    {
        return $this->selectQueryCounter->queries;
    }

    public function insert(string $tableName, array $data, array $types = []) : void
    {
        $this->connection->insert($tableName, $data, $types);
    }

    public function numberOfExecutedInsertQueries() : int
    {
        return $this->insertQueryCounter->count;
    }

    public function numberOfExecutedSelectQueries() : int
    {
        return $this->selectQueryCounter->count;
    }

    public function resetInsertQueryCounter() : void
    {
        $this->insertQueryCounter->reset();
    }

    public function resetSelectQueryCounter() : void
    {
        $this->selectQueryCounter->reset();
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
