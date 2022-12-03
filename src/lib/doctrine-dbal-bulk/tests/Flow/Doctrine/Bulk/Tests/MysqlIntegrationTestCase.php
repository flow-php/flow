<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests;

use Doctrine\DBAL\DriverManager;
use Flow\Doctrine\Bulk\Tests\Context\DatabaseContext;
use PHPUnit\Framework\TestCase;

abstract class MysqlIntegrationTestCase extends TestCase
{
    protected DatabaseContext $mysqlDatabaseContext;

    protected function setUp() : void
    {
        $this->mysqlDatabaseContext = new DatabaseContext(DriverManager::getConnection(['url' => \getenv('MYSQL_DATABASE_URL')]));
        $this->mysqlDatabaseContext->connection()->getDatabasePlatform()->registerDoctrineTypeMapping('enum', 'string');
    }

    protected function tearDown() : void
    {
        $this->mysqlDatabaseContext->dropAllTables();
    }
}
