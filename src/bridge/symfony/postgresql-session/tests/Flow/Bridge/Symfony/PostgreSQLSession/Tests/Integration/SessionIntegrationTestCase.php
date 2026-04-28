<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Tests\Integration;

use PHPUnit\Framework\TestCase;

abstract class SessionIntegrationTestCase extends TestCase
{
    private ?SessionTestContext $context = null;

    protected function setUp() : void
    {
        if (!\extension_loaded('pgsql')) {
            static::markTestSkipped('ext-pgsql is not available');
        }

        $this->context = new SessionTestContext();
        $this->context->dropSessionTable('sessions');
        $this->context->createSessionTable('sessions');
    }

    protected function tearDown() : void
    {
        if ($this->context !== null) {
            $this->context->dropSessionTable('sessions');
            $this->context->close();
            $this->context = null;
        }
    }

    protected function sessionContext() : SessionTestContext
    {
        if ($this->context === null) {
            static::fail('SessionTestContext not initialized. Ensure setUp() was called.');
        }

        return $this->context;
    }
}
