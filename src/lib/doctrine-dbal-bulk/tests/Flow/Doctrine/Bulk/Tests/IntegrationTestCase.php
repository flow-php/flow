<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests;

use Flow\Doctrine\Bulk\Tests\Context\DatabaseContext;
use Flow\Doctrine\Bulk\Tests\Context\ProxyLogger;
use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    protected DatabaseContext $databaseContext;

    protected ProxyLogger $logger;

    protected function setUp(): void
    {
        $this->logger = new ProxyLogger();
    }

    protected function tearDown(): void
    {
        $this->databaseContext->dropAllTables();
    }

    public function executedQueriesCount(): int
    {
        return $this->logger->count;
    }
}
