<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Logging\Middleware;
use Doctrine\DBAL\Tools\DsnParser;
use Flow\ETL\Adapter\Doctrine\Tests\Context\DatabaseContext;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InsertQueryCounter;
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
