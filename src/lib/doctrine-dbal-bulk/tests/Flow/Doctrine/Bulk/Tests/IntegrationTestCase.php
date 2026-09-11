<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests;

use Flow\Doctrine\Bulk\Tests\Context\DatabaseContext;
use Flow\Doctrine\Bulk\Tests\Context\ProxyLogger;
use Flow\Doctrine\Bulk\Tests\Double\SpyMiddleware;
use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    protected DatabaseContext $databaseContext;

    protected ProxyLogger $logger;

    protected SpyMiddleware $spy;

    protected function setUp(): void
    {
        $this->logger = new ProxyLogger();
        $this->spy = new SpyMiddleware();
    }

    protected function tearDown(): void
    {
        $this->databaseContext->dropAllTables();
    }

    public function executedQueriesCount(): int
    {
        return $this->logger->count;
    }

    public function preparedStatementsCount(): int
    {
        return $this->spy->prepares;
    }
}
