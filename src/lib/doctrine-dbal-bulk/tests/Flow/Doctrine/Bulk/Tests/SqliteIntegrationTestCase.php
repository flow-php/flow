<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Logging\Middleware;
use Flow\Doctrine\Bulk\Tests\Context\DatabaseContext;

abstract class SqliteIntegrationTestCase extends IntegrationTestCase
{
    protected function setUp() : void
    {
        $this->databaseContext = new DatabaseContext(
            DriverManager::getConnection(
                ['url' => \getenv('SQLITE_DATABASE_URL')],
                (new Configuration())->setMiddlewares([new Middleware($this->logger)])
            )
        );
    }
}
