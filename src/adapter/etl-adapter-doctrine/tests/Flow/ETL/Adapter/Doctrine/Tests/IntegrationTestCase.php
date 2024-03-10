<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests;

use Doctrine\DBAL\Logging\Middleware;
use Doctrine\DBAL\Tools\DsnParser;
use Doctrine\DBAL\{Configuration, DriverManager};
use Flow\ETL\Adapter\Doctrine\Tests\Context\{DatabaseContext, InsertQueryCounter};
use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    protected DatabaseContext $pgsqlDatabaseContext;

    protected function setUp() : void
    {
        $logger = new InsertQueryCounter();

        $this->pgsqlDatabaseContext = new DatabaseContext(
            DriverManager::getConnection(
                $this->connectionParams(),
                (new Configuration())->setMiddlewares([new Middleware($logger)])
            ),
            $logger
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
