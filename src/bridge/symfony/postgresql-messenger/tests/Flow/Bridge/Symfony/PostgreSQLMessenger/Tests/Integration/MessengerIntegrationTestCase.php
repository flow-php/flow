<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Integration;

use PHPUnit\Framework\TestCase;

abstract class MessengerIntegrationTestCase extends TestCase
{
    private ?MessengerTestContext $context = null;

    protected function setUp() : void
    {
        if (!\extension_loaded('pgsql')) {
            static::markTestSkipped('ext-pgsql is not available');
        }

        $this->context = new MessengerTestContext();
        $this->context->dropMessengerTable('messenger_messages');
        $this->context->createMessengerTable('messenger_messages');
    }

    protected function tearDown() : void
    {
        if ($this->context !== null) {
            $this->context->dropMessengerTable('messenger_messages');
            $this->context->close();
            $this->context = null;
        }
    }

    protected function messengerContext() : MessengerTestContext
    {
        if ($this->context === null) {
            static::fail('MessengerTestContext not initialized. Ensure setUp() was called.');
        }

        return $this->context;
    }
}
