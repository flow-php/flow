<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Unit;

use Flow\Bridge\PHPUnit\PostgreSQL\{SkipTransactionRollback, StaticClient};
use Flow\Bridge\PHPUnit\PostgreSQL\Tests\Context\StaticClientContext;
use Flow\Bridge\PHPUnit\PostgreSQL\Tests\Double\FakeClient;
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

    public function test_begin_transaction_skips_closed_clients() : void
    {
        $fakeClient = new FakeClient();
        $fakeClient->connected = false;

        StaticClientContext::injectClient('test-key', $fakeClient);

        StaticClient::beginTransaction();

        self::assertSame(0, $fakeClient->transactionLevel);
    }

    public function test_begin_transaction_works_on_connected_clients() : void
    {
        $fakeClient = new FakeClient();

        StaticClientContext::injectClient('test-key', $fakeClient);

        StaticClient::beginTransaction();

        self::assertSame(1, $fakeClient->transactionLevel);
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

    public function test_rollback_skips_closed_clients() : void
    {
        $fakeClient = new FakeClient();
        $fakeClient->connected = false;

        StaticClientContext::injectClient('test-key', $fakeClient);

        StaticClient::rollBack();

        self::assertSame(0, $fakeClient->transactionLevel);
    }

    public function test_rollback_works_on_connected_clients() : void
    {
        $fakeClient = new FakeClient();
        $fakeClient->transactionLevel = 1;

        StaticClientContext::injectClient('test-key', $fakeClient);

        StaticClient::rollBack();

        self::assertSame(0, $fakeClient->transactionLevel);
    }
}
