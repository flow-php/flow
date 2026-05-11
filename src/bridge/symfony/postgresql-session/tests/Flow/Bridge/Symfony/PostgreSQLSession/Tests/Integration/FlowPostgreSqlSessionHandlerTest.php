<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Tests\Integration;

use Flow\Bridge\Symfony\PostgreSQLSession\FlowPostgreSqlSessionHandler;

final class FlowPostgreSqlSessionHandlerTest extends SessionIntegrationTestCase
{
    public function test_close_after_gc_purges_expired_rows(): void
    {
        $writer = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, ['ttl' => 3600]);
        $writer->write('sid-expire', 'soon');
        $writer->write('sid-keep', 'forever');
        $writer->close();
        $this->sessionContext()->markSessionExpired('sid-expire');

        $gc = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters);
        $gc->gc(0);
        $gc->close();

        $reader = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE,
        ]);
        static::assertSame('', $reader->read('sid-expire'));
        static::assertSame('forever', $reader->read('sid-keep'));
        $reader->close();
    }

    public function test_destroy_removes_row(): void
    {
        $handler = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters);
        $handler->open('', 'PHPSESSID');
        $handler->write('sid-destroy', 'data');
        $handler->close();

        $reader = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE,
        ]);
        static::assertSame('data', $reader->read('sid-destroy'));
        $reader->close();

        $destroyer = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters);
        $destroyer->open('', 'PHPSESSID');
        static::assertTrue($destroyer->destroy('sid-destroy'));
        $destroyer->close();

        $verifier = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE,
        ]);
        static::assertSame('', $verifier->read('sid-destroy'));
        $verifier->close();
    }

    public function test_purge_all_removes_every_row(): void
    {
        $writer = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters);
        $writer->write('a', 'one');
        $writer->write('b', 'two');
        $writer->close();

        $purger = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters);
        static::assertSame(0, $purger->purgeAll());
        $purger->close();

        $reader = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE,
        ]);
        static::assertSame('', $reader->read('a'));
        static::assertSame('', $reader->read('b'));
        $reader->close();
    }

    public function test_purge_expired_returns_count(): void
    {
        $writer = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, ['ttl' => 3600]);
        $writer->write('expired-1', 'x');
        $writer->write('expired-2', 'y');
        $writer->write('alive', 'z');
        $writer->close();
        $this->sessionContext()->markSessionExpired('expired-1');
        $this->sessionContext()->markSessionExpired('expired-2');

        $purger = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters);
        static::assertSame(2, $purger->purgeExpired());
        $purger->close();

        $reader = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE,
        ]);
        static::assertSame('z', $reader->read('alive'));
        $reader->close();
    }

    public function test_read_returns_empty_when_session_already_expired(): void
    {
        $writer = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters);
        $writer->write('sid-stale', 'old-payload');
        $writer->close();
        $this->sessionContext()->markSessionExpired('sid-stale');

        $reader = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE,
        ]);
        static::assertSame('', $reader->read('sid-stale'));
        $reader->close();
    }

    public function test_update_timestamp_extends_stored_lifetime(): void
    {
        $shortLived = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, ['ttl' => 1]);
        $shortLived->write('sid-touch', 'original');
        $shortLived->close();
        $beforeLifetime = $this->sessionContext()->fetchSessionLifetime('sid-touch');

        $longLived = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, ['ttl' => 3600]);
        $longLived->updateTimestamp('sid-touch', 'unused');
        $longLived->close();
        $afterLifetime = $this->sessionContext()->fetchSessionLifetime('sid-touch');

        static::assertGreaterThan($beforeLifetime + 100, $afterLifetime);

        $reader = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE,
        ]);
        static::assertSame('original', $reader->read('sid-touch'));
        $reader->close();
    }

    public function test_write_then_read_round_trip(): void
    {
        $writer = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters);
        static::assertTrue($writer->write('sid-1', 'hello world'));
        $writer->close();

        $reader = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE,
        ]);
        static::assertSame('hello world', $reader->read('sid-1'));
        $reader->close();
    }

    public function test_write_then_read_round_trip_with_advisory_lock(): void
    {
        $writer = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_ADVISORY,
        ]);

        static::assertTrue($writer->write('sid-adv', 'advisory-payload'));
        static::assertTrue($writer->close());

        $reader = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_ADVISORY,
        ]);
        static::assertSame('advisory-payload', $reader->read('sid-adv'));
        $reader->close();
    }

    public function test_write_then_read_round_trip_with_transactional_lock(): void
    {
        $writer = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_TRANSACTIONAL,
        ]);

        $writer->read('sid-tx');
        static::assertTrue($writer->write('sid-tx', 'tx-payload'));
        $writer->close();

        $reader = new FlowPostgreSqlSessionHandler($this->sessionContext()->connectionParameters, [
            'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE,
        ]);
        static::assertSame('tx-payload', $reader->read('sid-tx'));
        $reader->close();
    }
}
