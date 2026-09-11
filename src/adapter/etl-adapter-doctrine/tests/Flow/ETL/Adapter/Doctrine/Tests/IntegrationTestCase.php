<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Logging\Middleware;
use Doctrine\DBAL\Tools\DsnParser;
use Flow\ETL\Adapter\Doctrine\Tests\Context\DatabaseContext;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InsertQueryCounter;
use Flow\ETL\Adapter\Doctrine\Tests\Context\SelectQueryCounter;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Types\DSL\type_string;
use function getenv;
use function str_starts_with;

/**
 * @import-type Params from DriverManager
 */
abstract class IntegrationTestCase extends FlowTestCase
{
    protected DatabaseContext $mysqlDatabaseContext;

    protected DatabaseContext $pgsqlDatabaseContext;

    protected DatabaseContext $sqliteDatabaseContext;

    protected function setUp(): void
    {
        $insertQueryCounter = new InsertQueryCounter();
        $selectQueryCounter = new SelectQueryCounter();

        $pgsqlParams = $this->postgresqlConnectionParams();
        $this->pgsqlDatabaseContext = new DatabaseContext(
            DriverManager::getConnection($pgsqlParams, (new Configuration())->setMiddlewares([
                new Middleware($insertQueryCounter),
                new Middleware($selectQueryCounter),
            ])),
            $insertQueryCounter,
            $selectQueryCounter,
        );

        $mysqlParams = $this->mysqlConnectionParams();
        $this->mysqlDatabaseContext = new DatabaseContext(
            DriverManager::getConnection($mysqlParams, (new Configuration())->setMiddlewares([
                new Middleware($insertQueryCounter),
                new Middleware($selectQueryCounter),
            ])),
            $insertQueryCounter,
            $selectQueryCounter,
        );

        $sqliteParams = $this->sqliteConnectionParams();
        $this->sqliteDatabaseContext = new DatabaseContext(
            DriverManager::getConnection($sqliteParams, (new Configuration())->setMiddlewares([
                new Middleware($insertQueryCounter),
                new Middleware($selectQueryCounter),
            ])),
            $insertQueryCounter,
            $selectQueryCounter,
        );
    }

    protected function tearDown(): void
    {
        $this->pgsqlDatabaseContext->dropAllTables();
        $this->mysqlDatabaseContext->dropAllTables();
        $this->sqliteDatabaseContext->dropAllTables();

        $this->pgsqlDatabaseContext->connection()->close();
        $this->mysqlDatabaseContext->connection()->close();
        $this->sqliteDatabaseContext->connection()->close();
    }

    /**
     * @return Params
     */
    protected function mysqlConnectionParams(): array
    {
        return (new DsnParser(['mysql' => 'mysqli']))->parse(getenv('MYSQL_DATABASE_URL') ?: '');
    }

    /**
     * @return Params
     */
    protected function postgresqlConnectionParams(): array
    {
        return (new DsnParser(['postgresql' => 'pdo_pgsql']))->parse(getenv('PGSQL_DATABASE_URL') ?: '');
    }

    /**
     * @return Params
     */
    protected function sqliteConnectionParams(): array
    {
        $path = type_string()->assert(getenv('SQLITE_DATABASE_PATH'));
        $folder = pathinfo($path, PATHINFO_DIRNAME);

        if (!is_dir($folder)) {
            @mkdir($folder, 0777, true);
        }

        return (new DsnParser(['sqlite' => 'sqlite3']))->parse(
            'sqlite3://' . (str_starts_with($path, '/') ? '/' : '') . $path,
        );
    }
}
