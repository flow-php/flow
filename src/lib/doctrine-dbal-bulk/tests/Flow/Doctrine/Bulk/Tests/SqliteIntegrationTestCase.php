<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests;

use Doctrine\DBAL\Logging\Middleware;
use Doctrine\DBAL\Tools\DsnParser;
use Doctrine\DBAL\{Configuration, DriverManager};
use Flow\Doctrine\Bulk\Tests\Context\DatabaseContext;

abstract class SqliteIntegrationTestCase extends IntegrationTestCase
{
    protected function setUp() : void
    {
        $this->databaseContext = new DatabaseContext(
            DriverManager::getConnection(
                (new DsnParser(['sqlite' => 'pdo_sqlite']))->parse(\getenv('SQLITE_DATABASE_URL') ?: ''),
                (new Configuration())->setMiddlewares([new Middleware($this->logger)])
            )
        );
    }
}
