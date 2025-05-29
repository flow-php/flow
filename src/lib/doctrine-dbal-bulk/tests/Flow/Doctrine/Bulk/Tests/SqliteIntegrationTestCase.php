<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests;

use function Flow\Types\DSL\type_string;
use Doctrine\DBAL\{Configuration, DriverManager};
use Doctrine\DBAL\Logging\Middleware;
use Doctrine\DBAL\Tools\DsnParser;
use Flow\Doctrine\Bulk\Tests\Context\DatabaseContext;

abstract class SqliteIntegrationTestCase extends IntegrationTestCase
{
    protected function setUp() : void
    {
        $path = type_string()->assert(\getenv('SQLITE_DATABASE_PATH'));
        $folder = pathinfo($path, PATHINFO_DIRNAME);

        if (!is_dir($folder)) {
            @mkdir($folder, 0777, true);
        }

        $this->databaseContext = new DatabaseContext(
            DriverManager::getConnection(
                (new DsnParser(['sqlite' => 'sqlite3']))->parse('sqlite3://' . $path),
                (new Configuration())->setMiddlewares([new Middleware($this->logger)])
            )
        );
    }
}
