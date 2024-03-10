<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests;

use Doctrine\DBAL\Logging\Middleware;
use Doctrine\DBAL\Tools\DsnParser;
use Doctrine\DBAL\{Configuration, DriverManager};
use Flow\Doctrine\Bulk\Tests\Context\DatabaseContext;

abstract class PostgreSqlIntegrationTestCase extends IntegrationTestCase
{
    protected function setUp() : void
    {
        $this->databaseContext = new DatabaseContext(
            DriverManager::getConnection(
                (new DsnParser(['pgsql' => 'pdo_pgsql']))->parse(\getenv('PGSQL_DATABASE_URL') ?: ''),
                (new Configuration())->setMiddlewares([new Middleware($this->logger)])
            )
        );
    }
}
