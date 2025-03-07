<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests;

use Doctrine\DBAL\Logging\Middleware;
use Doctrine\DBAL\Tools\DsnParser;
use Doctrine\DBAL\{Configuration, DriverManager};
use Flow\ETL\Adapter\Doctrine\Tests\Context\{DatabaseContext, InsertQueryCounter, SelectQueryCounter};
use Flow\ETL\Tests\FlowTestCase;

abstract class IntegrationTestCase extends FlowTestCase
{
    protected DatabaseContext $mysqlDatabaseContext;

    protected DatabaseContext $pgsqlDatabaseContext;

    protected function setUp() : void
    {
        $insertQueryCounter = new InsertQueryCounter();
        $selectQueryCounter = new SelectQueryCounter();

        $this->pgsqlDatabaseContext = new DatabaseContext(
            DriverManager::getConnection(
                $this->postgresqlConnectionParams(),
                (new Configuration())->setMiddlewares([new Middleware($insertQueryCounter), new Middleware($selectQueryCounter)])
            ),
            $insertQueryCounter,
            $selectQueryCounter
        );

        $this->mysqlDatabaseContext = new DatabaseContext(
            DriverManager::getConnection(
                $this->mysqlConnectionParams(),
                (new Configuration())->setMiddlewares([new Middleware($insertQueryCounter), new Middleware($selectQueryCounter)])
            ),
            $insertQueryCounter,
            $selectQueryCounter
        );
    }

    protected function tearDown() : void
    {
        $this->pgsqlDatabaseContext->dropAllTables();
    }

    protected function mysqlConnectionParams() : array
    {
        return (new DsnParser(['mysql' => 'mysqli']))->parse(\getenv('MYSQL_DATABASE_URL') ?: '');
    }

    protected function postgresqlConnectionParams() : array
    {
        return (new DsnParser(['postgresql' => 'pdo_pgsql']))->parse(\getenv('PGSQL_DATABASE_URL') ?: '');
    }
}
