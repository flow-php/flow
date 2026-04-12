<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Unit;

use Flow\Bridge\PHPUnit\PostgreSQL\{SkipTransactionRollback, StaticClient};
use PHPUnit\Framework\TestCase;

#[SkipTransactionRollback]
final class StaticClientTest extends TestCase
{
    protected function setUp() : void
    {
        StaticClient::reset();
    }

    protected function tearDown() : void
    {
        StaticClient::reset();
    }

    public function test_disable_sets_disabled_state() : void
    {
        StaticClient::enable();
        StaticClient::disable();

        self::assertFalse(StaticClient::isEnabled());
    }

    public function test_enable_sets_enabled_state() : void
    {
        StaticClient::enable();

        self::assertTrue(StaticClient::isEnabled());
    }

    public function test_is_disabled_by_default() : void
    {
        self::assertFalse(StaticClient::isEnabled());
    }

    public function test_reset_clears_all_state() : void
    {
        StaticClient::enable();
        self::assertTrue(StaticClient::isEnabled());

        StaticClient::reset();

        self::assertFalse(StaticClient::isEnabled());
    }
}
