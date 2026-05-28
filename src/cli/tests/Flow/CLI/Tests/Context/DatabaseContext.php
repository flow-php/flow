<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Context;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Tools\DsnParser;

use function getenv;

final readonly class DatabaseContext
{
    private Connection $connection;

    public function __construct()
    {
        $this->connection = DriverManager::getConnection((new DsnParser(['postgresql' => 'pdo_pgsql']))->parse(
            getenv('PGSQL_DATABASE_URL') ?: '',
        ));
    }

    public function createTable(Table $table): void
    {
        $schemaManager = $this->connection->createSchemaManager();

        $tableName = $table->getObjectName()->getUnqualifiedName()->getValue();

        if ($schemaManager->tablesExist([$tableName])) {
            $schemaManager->dropTable($tableName);
        }

        $schemaManager->createTable($table);
    }

    public function dropAllTables(): void
    {
        $schemaManager = $this->connection->createSchemaManager();

        foreach ($schemaManager->introspectTables() as $table) {
            $schemaManager->dropTable($table->getObjectName()->getUnqualifiedName()->getValue());
        }
    }
}
