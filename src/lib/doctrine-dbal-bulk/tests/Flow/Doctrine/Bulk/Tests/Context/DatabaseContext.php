<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Context;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Table;

use function str_contains;

final readonly class DatabaseContext
{
    public function __construct(
        private Connection $connection,
    ) {}

    public function connection(): Connection
    {
        return $this->connection;
    }

    public function createTable(Table $table): void
    {
        $schemaManager = $this->connection->createSchemaManager();

        // @mago-expect analysis:deprecated-method
        if ($schemaManager->tablesExist([$table->getName()])) {
            // @mago-expect analysis:deprecated-method
            $schemaManager->dropTable($table->getName());
        }

        $schemaManager->createTable($table);
    }

    public function dropAllTables(): void
    {
        $schemaManager = $this->connection->createSchemaManager();
        $platform = $this->connection->getDatabasePlatform();

        foreach ($schemaManager->introspectTableNames() as $tableName) {
            $name = $tableName->getUnqualifiedName()->getValue();

            if (str_contains($name, 'innodb')) {
                continue;
            }

            if (str_contains($name, 'mysql')) {
                continue;
            }

            $schemaManager->dropTable($tableName->toSQL($platform));
        }
    }

    public function selectAll(string $tableName, string $orderBy = 'id'): array
    {
        return $this->connection->fetchAllAssociative(
            $this->connection
                ->createQueryBuilder()
                ->select('*')
                ->from($tableName)
                ->orderBy($orderBy)
                ->getSQL(),
        );
    }

    public function tableCount(string $tableName): int
    {
        return (int) $this->connection->fetchOne(
            $this->connection->createQueryBuilder()->select('COUNT(*)')->from($tableName)->getSQL(),
        );
    }
}
