<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Context;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Table;

final class DatabaseContext
{
    public function __construct(private readonly Connection $connection)
    {
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
        foreach ($this->connection->createSchemaManager()->listTables() as $table) {
            if (\str_contains($table->getName(), 'innodb')) {
                continue;
            }

            if (\str_contains($table->getName(), 'mysql')) {
                continue;
            }

            $this->connection->createSchemaManager()->dropTable($table->getName());
        }
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
