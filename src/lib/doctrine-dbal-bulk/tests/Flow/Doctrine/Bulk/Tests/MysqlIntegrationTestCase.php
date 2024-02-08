<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Logging\Middleware;
use Doctrine\DBAL\Tools\DsnParser;
use Flow\Doctrine\Bulk\Tests\Context\DatabaseContext;

abstract class MysqlIntegrationTestCase extends IntegrationTestCase
{
    protected function setUp() : void
    {
        $this->databaseContext = new DatabaseContext(
            DriverManager::getConnection(
                (new DsnParser(['mysql' => 'pdo_mysql']))->parse(\getenv('MYSQL_DATABASE_URL') ?: ''),
                (new Configuration())->setMiddlewares([new Middleware($this->logger)])
            )
        );
        $this->databaseContext->connection()->getDatabasePlatform()->registerDoctrineTypeMapping('enum', 'string');
    }
}
