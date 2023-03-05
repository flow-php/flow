<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests;

use Doctrine\DBAL\DriverManager;
use Flow\Doctrine\Bulk\Tests\Context\DatabaseContext;
use PHPUnit\Framework\TestCase;

abstract class PostgreSqlIntegrationTestCase extends TestCase
{
    protected DatabaseContext $databaseContext;

    protected function setUp() : void
    {
        $this->databaseContext = new DatabaseContext(DriverManager::getConnection(['url' => \getenv('PGSQL_DATABASE_URL')]));
    }

    protected function tearDown() : void
    {
        $this->databaseContext->dropAllTables();
    }
}
