<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use PHPUnit\Framework\TestCase;

abstract class CommandTestCase extends TestCase
{
    protected CommandTestContext $context;

    protected function setUp() : void
    {
        $this->context = new CommandTestContext();
        $this->context->bootWithMigrations();
    }

    protected function tearDown() : void
    {
        $this->context->shutdown();
    }
}
