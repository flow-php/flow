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
    protected DatabaseContext $pgsqlDatabaseContext;

    protected function setUp() : void
    {
        $insertQueryCounter = new InsertQueryCounter();
        $selectQueryCounter = new SelectQueryCounter();

        $this->pgsqlDatabaseContext = new DatabaseContext(
            DriverManager::getConnection(
                $this->connectionParams(),
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

    protected function connectionParams() : array
    {
        return (new DsnParser(['postgresql' => 'pdo_pgsql']))->parse(\getenv('PGSQL_DATABASE_URL') ?: '');
    }
}
