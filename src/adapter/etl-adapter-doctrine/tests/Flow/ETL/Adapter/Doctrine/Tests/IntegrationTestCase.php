<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests;

use function Flow\Types\DSL\type_string;
use Doctrine\DBAL\{Configuration, DriverManager};
use Doctrine\DBAL\Logging\Middleware;
use Doctrine\DBAL\Tools\DsnParser;
use Flow\ETL\Adapter\Doctrine\Tests\Context\{DatabaseContext, InsertQueryCounter, SelectQueryCounter};
use Flow\ETL\Tests\FlowTestCase;

abstract class IntegrationTestCase extends FlowTestCase
{
    protected DatabaseContext $mysqlDatabaseContext;

    protected DatabaseContext $pgsqlDatabaseContext;

    protected DatabaseContext $sqliteDatabaseContext;

    protected function setUp() : void
    {
        $insertQueryCounter = new InsertQueryCounter();
        $selectQueryCounter = new SelectQueryCounter();

        $this->pgsqlDatabaseContext = new DatabaseContext(
            DriverManager::getConnection(
                /** @phpstan-ignore-next-line */
                $this->postgresqlConnectionParams(),
                (new Configuration())->setMiddlewares([new Middleware($insertQueryCounter), new Middleware($selectQueryCounter)])
            ),
            $insertQueryCounter,
            $selectQueryCounter
        );

        $this->mysqlDatabaseContext = new DatabaseContext(
            DriverManager::getConnection(
                /** @phpstan-ignore-next-line */
                $this->mysqlConnectionParams(),
                (new Configuration())->setMiddlewares([new Middleware($insertQueryCounter), new Middleware($selectQueryCounter)])
            ),
            $insertQueryCounter,
            $selectQueryCounter
        );

        $this->sqliteDatabaseContext = new DatabaseContext(
            DriverManager::getConnection(
                /** @phpstan-ignore-next-line */
                $this->sqliteConnectionParams(),
                (new Configuration())->setMiddlewares([new Middleware($insertQueryCounter), new Middleware($selectQueryCounter)])
            ),
            $insertQueryCounter,
            $selectQueryCounter
        );
    }

    protected function tearDown() : void
    {
        $this->pgsqlDatabaseContext->dropAllTables();
        $this->mysqlDatabaseContext->dropAllTables();
        $this->sqliteDatabaseContext->dropAllTables();

        $this->pgsqlDatabaseContext->connection()->close();
        $this->mysqlDatabaseContext->connection()->close();
        $this->sqliteDatabaseContext->connection()->close();
    }

    /**
     * @return array<string, mixed>
     */
    protected function mysqlConnectionParams() : array
    {
        return (new DsnParser(['mysql' => 'mysqli']))->parse(\getenv('MYSQL_DATABASE_URL') ?: '');
    }

    /**
     * @return array<string, mixed>
     */
    protected function postgresqlConnectionParams() : array
    {
        return (new DsnParser(['postgresql' => 'pdo_pgsql']))->parse(\getenv('PGSQL_DATABASE_URL') ?: '');
    }

    /**
     * @return array<string, mixed>
     */
    protected function sqliteConnectionParams() : array
    {
        $path = type_string()->assert(\getenv('SQLITE_DATABASE_PATH'));
        $folder = pathinfo($path, PATHINFO_DIRNAME);

        if (!is_dir($folder)) {
            @mkdir($folder, 0777, true);
        }

        return (new DsnParser(['sqlite' => 'sqlite3']))->parse('sqlite3://' . $path);
    }
}
