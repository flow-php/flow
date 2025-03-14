<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Context;

use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Tools\DsnParser;
use Doctrine\DBAL\{Connection, DriverManager};

final readonly class DatabaseContext
{
    private Connection $connection;

    public function __construct()
    {
        $this->connection = DriverManager::getConnection(
            (new DsnParser(['postgresql' => 'pdo_pgsql']))
                ->parse(\getenv('PGSQL_DATABASE_URL') ?: '')
        );
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
}
