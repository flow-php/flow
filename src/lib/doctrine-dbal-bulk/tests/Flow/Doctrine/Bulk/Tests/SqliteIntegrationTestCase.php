<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Logging\Middleware;
use Doctrine\DBAL\Tools\DsnParser;
use Flow\Doctrine\Bulk\Tests\Context\DatabaseContext;

use function Flow\Types\DSL\type_string;
use function getenv;
use function str_starts_with;

abstract class SqliteIntegrationTestCase extends IntegrationTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $path = type_string()->assert(getenv('SQLITE_DATABASE_PATH'));
        $folder = pathinfo($path, PATHINFO_DIRNAME);

        if (!is_dir($folder)) {
            @mkdir($folder, 0777, true);
        }

        $this->databaseContext = new DatabaseContext(DriverManager::getConnection(
            (new DsnParser(['sqlite' => 'sqlite3']))->parse(
                'sqlite3://' . (str_starts_with($path, '/') ? '/' : '') . $path,
            ),
            (new Configuration())->setMiddlewares([new Middleware($this->logger), $this->spy]),
        ));
    }
}
