<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Context;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Table;

final class DatabaseContext
{
    private readonly InsertQueryCounter $sqlLogger;

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
        return $this->sqlLogger->count;
    }

    public function dropAllTables() : void
    {
        foreach ($this->connection->getSchemaManager()->listTables() as $table) {
            $this->connection->getSchemaManager()->dropTable($table->getName());
        }
    }
}
