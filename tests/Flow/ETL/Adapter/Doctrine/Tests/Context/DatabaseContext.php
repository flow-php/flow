<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Logging\SQLLogger;

final class DatabaseContext
{
    private Connection $connection;

    private SQLLogger $sqlLogger;

    /**
     * @var string[]
     */
    private array $createdTables;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->sqlLogger = new InsertQueryCounter();
        $this->createdTables = [];

        $this->connection->getConfiguration()->setSQLLogger($this->sqlLogger);
    }

    public function connection() : Connection
    {
        return $this->connection;
    }

    public function createTestTable(string $tableName) : void
    {
        $this->connection->executeQuery("DROP TABLE IF EXISTS {$tableName}");
        $this->connection->executeQuery("CREATE TABLE {$tableName} (id INT NOT NULL, name VARCHAR(255) NOT NULL, description VARCHAR(255) NOT NULL, PRIMARY KEY(id))");

        $this->createdTables[] = $tableName;
    }

    public function selectAll(string $tableName) : array
    {
        return $this->connection->fetchAllAssociative("SELECT * FROM {$tableName} ORDER BY id");
    }

    public function tableCount(string $tableName) : int
    {
        return (int) $this->connection->fetchOne("SELECT COUNT(*) FROM {$tableName}");
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
        foreach ($this->createdTables as $table) {
            $this->connection->executeQuery("DROP TABLE {$table}");
        }
    }
}
