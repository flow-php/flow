<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Doctrine\DBAL\{Connection, ParameterType};
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;

final class DatabaseContext
{
    /**
     * @var array<string>
     */
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

    /**
     * @return array<string>
     */
    public function executedSelectQueries() : array
    {
        return $this->selectQueryCounter->queries;
    }

    /**
     * @param array<string, mixed> $data
     * @param array<string, mixed> $types
     */
    public function insert(string $tableName, array $data, array $types = []) : void
    {
        /** @var array<int<0, max>|string, ParameterType|string|Type> $doctrineTypes */
        $doctrineTypes = $types;
        $this->connection->insert($tableName, $data, $doctrineTypes);
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

    /**
     * @return array<array<string, mixed>>
     */
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
        $result = $this->connection->fetchOne(
            $this
                ->connection
                ->createQueryBuilder()
                ->select('COUNT(*)')
                ->from($tableName)
                ->getSQL()
        );

        return \is_numeric($result) ? (int) $result : 0;
    }
}
