<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Logging\SQLLogger;
use Doctrine\DBAL\Schema\Table;

final class DatabaseContext
{
    private readonly SQLLogger $sqlLogger;

    public function __construct(private readonly Connection $connection)
    {
        $this->sqlLogger = new InsertQueryCounter();

        $this->connection->getConfiguration()->setSQLLogger($this->sqlLogger);
    }

    public function connection() : Connection
    {
        return $this->connection;
    }

    public function createTable(Table $table) : void
    {
        $schemaManager = $this
            ->connection
            ->getSchemaManager();

        if ($schemaManager->tablesExist($table->getName())) {
            $schemaManager->dropTable($table->getName());
        }

        $schemaManager->createTable($table);
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

    public function numberOfExecutedInsertQueries() : int
    {
        if ($this->sqlLogger instanceof InsertQueryCounter) {
            return $this->sqlLogger->count;
        }

        return 0;
    }

    public function dropAllTables() : void
    {
        foreach ($this->connection->getSchemaManager()->listTables() as $table) {
            $this->connection->getSchemaManager()->dropTable($table->getName());
        }
    }
}
