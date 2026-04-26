<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Tests\Integration;

use Flow\Bridge\Symfony\PostgreSQLSession\FlowPostgreSqlSessionHandler;

final class FlowPostgreSqlSessionHandlerTest extends SessionIntegrationTestCase
{
    public function test_close_after_gc_purges_expired_rows() : void
    {
        $handler = new FlowPostgreSqlSessionHandler($this->sessionContext()->client, ['ttl' => 3600]);

        $handler->write('sid-expire', 'soon');
        $handler->write('sid-keep', 'forever');
        $this->sessionContext()->markSessionExpired('sid-expire');

        $handler->gc(0);
        $handler->close();

        self::assertSame('', (new FlowPostgreSqlSessionHandler($this->sessionContext()->client))->read('sid-expire'));
        self::assertSame('forever', (new FlowPostgreSqlSessionHandler($this->sessionContext()->client))->read('sid-keep'));
    }

    public function test_destroy_removes_row() : void
    {
        $handler = new FlowPostgreSqlSessionHandler($this->sessionContext()->client);
        $handler->open('', 'PHPSESSID');
        $handler->write('sid-destroy', 'data');

        self::assertSame('data', (new FlowPostgreSqlSessionHandler($this->sessionContext()->client))->read('sid-destroy'));

        self::assertTrue($handler->destroy('sid-destroy'));
        self::assertSame('', (new FlowPostgreSqlSessionHandler($this->sessionContext()->client))->read('sid-destroy'));
    }

    public function test_purge_all_removes_every_row() : void
    {
        $handler = new FlowPostgreSqlSessionHandler($this->sessionContext()->client);
        $handler->write('a', 'one');
        $handler->write('b', 'two');

        self::assertSame(0, $handler->purgeAll());

        self::assertSame('', (new FlowPostgreSqlSessionHandler($this->sessionContext()->client))->read('a'));
        self::assertSame('', (new FlowPostgreSqlSessionHandler($this->sessionContext()->client))->read('b'));
    }

    public function test_purge_expired_returns_count() : void
    {
        $handler = new FlowPostgreSqlSessionHandler($this->sessionContext()->client, ['ttl' => 3600]);

        $handler->write('expired-1', 'x');
        $handler->write('expired-2', 'y');
        $handler->write('alive', 'z');
        $this->sessionContext()->markSessionExpired('expired-1');
        $this->sessionContext()->markSessionExpired('expired-2');

        self::assertSame(2, $handler->purgeExpired());
        self::assertSame('z', (new FlowPostgreSqlSessionHandler($this->sessionContext()->client))->read('alive'));
    }

    public function test_read_returns_empty_when_session_already_expired() : void
    {
        $handler = new FlowPostgreSqlSessionHandler($this->sessionContext()->client);
        $handler->write('sid-stale', 'old-payload');
        $this->sessionContext()->markSessionExpired('sid-stale');

        self::assertSame('', (new FlowPostgreSqlSessionHandler($this->sessionContext()->client))->read('sid-stale'));
    }

    public function test_update_timestamp_extends_stored_lifetime() : void
    {
        $shortLived = new FlowPostgreSqlSessionHandler($this->sessionContext()->client, ['ttl' => 1]);
        $shortLived->write('sid-touch', 'original');
        $beforeLifetime = $this->sessionContext()->fetchSessionLifetime('sid-touch');

        $longLived = new FlowPostgreSqlSessionHandler($this->sessionContext()->client, ['ttl' => 3600]);
        $longLived->updateTimestamp('sid-touch', 'unused');
        $afterLifetime = $this->sessionContext()->fetchSessionLifetime('sid-touch');

        self::assertGreaterThan($beforeLifetime + 100, $afterLifetime);
        self::assertSame('original', (new FlowPostgreSqlSessionHandler($this->sessionContext()->client))->read('sid-touch'));
    }

    public function test_write_then_read_round_trip() : void
    {
        $handler = new FlowPostgreSqlSessionHandler($this->sessionContext()->client);

        self::assertTrue($handler->write('sid-1', 'hello world'));

        self::assertSame('hello world', (new FlowPostgreSqlSessionHandler($this->sessionContext()->client))->read('sid-1'));
    }

    public function test_write_then_read_round_trip_with_advisory_lock() : void
    {
        $writer = new FlowPostgreSqlSessionHandler(
            $this->sessionContext()->client,
            ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_ADVISORY],
        );

        self::assertTrue($writer->write('sid-adv', 'advisory-payload'));
        self::assertTrue($writer->close());

        $reader = new FlowPostgreSqlSessionHandler(
            $this->sessionContext()->client,
            ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_ADVISORY],
        );
        self::assertSame('advisory-payload', $reader->read('sid-adv'));
        $reader->close();
    }

    public function test_write_then_read_round_trip_with_transactional_lock() : void
    {
        $writer = new FlowPostgreSqlSessionHandler(
            $this->sessionContext()->client,
            ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_TRANSACTIONAL],
        );

        $writer->read('sid-tx');
        self::assertTrue($writer->write('sid-tx', 'tx-payload'));

        $reader = new FlowPostgreSqlSessionHandler($this->sessionContext()->client);
        self::assertSame('tx-payload', $reader->read('sid-tx'));
    }
}
