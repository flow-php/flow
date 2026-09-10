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
        // @mago-expect analysis:deprecated-method
        foreach ($this->connection->createSchemaManager()->listTables() as $table) {
            // @mago-expect analysis:deprecated-method
            if (str_contains($table->getName(), 'innodb')) {
                continue;
            }

            // @mago-expect analysis:deprecated-method
            if (str_contains($table->getName(), 'mysql')) {
                continue;
            }

            // @mago-expect analysis:deprecated-method
            $this->connection->createSchemaManager()->dropTable($table->getName());
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
