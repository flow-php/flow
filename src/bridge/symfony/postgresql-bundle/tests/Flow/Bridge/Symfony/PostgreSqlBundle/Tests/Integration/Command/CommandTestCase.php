<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Context\FilesystemContext;
use PHPUnit\Framework\TestCase;

abstract class CommandTestCase extends TestCase
{
    protected CommandTestContext $context;

    protected FilesystemContext $fs;

    protected function setUp() : void
    {
        $this->context = new CommandTestContext();
        $this->context->bootWithMigrations();
        $this->fs = new FilesystemContext();
    }

    protected function tearDown() : void
    {
        $this->fs->cleanup();
        $this->context->shutdown();
    }
}
