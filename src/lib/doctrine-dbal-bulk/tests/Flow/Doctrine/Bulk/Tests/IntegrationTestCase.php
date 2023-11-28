<?php
declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests;

use Flow\Doctrine\Bulk\Tests\Context\DatabaseContext;
use Flow\Doctrine\Bulk\Tests\Context\ProxyLogger;
use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    protected DatabaseContext $databaseContext;

    protected readonly ProxyLogger $logger;

    public function __construct(string $name)
    {
        $this->logger = new ProxyLogger();

        parent::__construct($name);
    }

    protected function tearDown() : void
    {
        $this->databaseContext->dropAllTables();
    }

    public function executedQueriesCount() : int
    {
        return $this->logger->count;
    }
}
