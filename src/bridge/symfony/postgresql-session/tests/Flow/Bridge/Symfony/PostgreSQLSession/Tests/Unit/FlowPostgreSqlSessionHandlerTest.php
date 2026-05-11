<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLSession\Exception\SessionException;
use Flow\Bridge\Symfony\PostgreSQLSession\FlowPostgreSqlSessionHandler;
use Flow\Bridge\Symfony\PostgreSQLSession\Tests\Context\PostgreSqlSessionContext;
use PHPUnit\Framework\TestCase;

final class FlowPostgreSqlSessionHandlerTest extends TestCase
{
    private PostgreSqlSessionContext $context;

    protected function setUp(): void
    {
        $this->context = new PostgreSqlSessionContext();
    }

    public function test_close_after_gc_issues_delete_by_lifetime(): void
    {
        $handler = $this->context->handler();

        $handler->gc(0);
        $handler->close();

        static::assertCount(1, $this->context->client->executedQueries);
        $closeQuery = $this->context->client->executedQueries[0];
        static::assertStringContainsString('DELETE FROM', $closeQuery['sql']);
        static::assertStringContainsString('sess_lifetime', $closeQuery['sql']);
    }

    public function test_close_without_gc_does_not_delete(): void
    {
        $handler = $this->context->handler();

        $handler->close();

        static::assertSame([], $this->context->client->executedQueries);
    }

    public function test_constructor_accepts_advisory_lock_mode(): void
    {
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_ADVISORY]);

        static::assertInstanceOf(FlowPostgreSqlSessionHandler::class, $handler);
    }

    public function test_constructor_accepts_none_lock_mode(): void
    {
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        static::assertInstanceOf(FlowPostgreSqlSessionHandler::class, $handler);
    }

    public function test_constructor_rejects_invalid_lock_mode(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid lock_mode "99"');

        $this->context->handler(['lock_mode' => 99]);
    }

    public function test_destroy_commits_open_transaction_and_releases_lock(): void
    {
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_TRANSACTIONAL]);
        $this->context->client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => \PHP_INT_MAX];

        $handler->read('sid-9');
        $handler->destroy('sid-9');

        static::assertSame(['begin', 'commit'], $this->context->client->transactionEvents);
        static::assertSame(0, $this->context->client->transactionLevel);
    }

    public function test_destroy_emits_delete_by_id(): void
    {
        $handler = $this->context->handler();

        $deletes = $this->context->onlyQueries('DELETE', static function () use ($handler): void {
            $handler->destroy('sid-7');
        });

        static::assertCount(1, $deletes);
        static::assertStringContainsString('sess_id', $deletes[0]['sql']);
        static::assertSame(['sid-7'], $deletes[0]['parameters']);
    }

    public function test_gc_does_not_run_sql(): void
    {
        $handler = $this->context->handler();

        $result = $handler->gc(0);

        static::assertSame(0, $result);
        static::assertSame([], $this->context->client->executedQueries);
    }

    public function test_purge_all_emits_truncate(): void
    {
        $handler = $this->context->handler();

        $count = $handler->purgeAll();

        static::assertSame(0, $count);
        static::assertCount(1, $this->context->client->executedQueries);
        static::assertStringContainsString('TRUNCATE', $this->context->client->executedQueries[0]['sql']);
    }

    public function test_purge_expired_returns_affected_row_count(): void
    {
        $this->context->client->executeReturn = 7;
        $handler = $this->context->handler();

        static::assertSame(7, $handler->purgeExpired());
        static::assertStringContainsString('DELETE FROM', $this->context->client->executedQueries[0]['sql']);
        static::assertStringContainsString('sess_lifetime', $this->context->client->executedQueries[0]['sql']);
    }

    public function test_read_advisory_mode_acquires_lock_before_select(): void
    {
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_ADVISORY]);

        $handler->read('sid-5');

        static::assertGreaterThanOrEqual(2, \count($this->context->client->executedQueries));
        static::assertStringContainsString('pg_advisory_lock', $this->context->client->executedQueries[0]['sql']);
        static::assertStringContainsString('SELECT', $this->context->client->executedQueries[1]['sql']);
        static::assertStringContainsString('sess_data', $this->context->client->executedQueries[1]['sql']);
    }

    public function test_read_emits_for_update_in_transactional_mode(): void
    {
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_TRANSACTIONAL]);

        $handler->read('sid-1');

        static::assertSame(1, $this->context->client->transactionLevel);
        static::assertCount(1, $this->context->client->executedQueries);
        static::assertStringContainsString('FOR UPDATE', $this->context->client->executedQueries[0]['sql']);
    }

    public function test_read_returns_empty_string_when_session_expired(): void
    {
        $this->context->client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => \time() - 100];
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        static::assertSame('', $handler->read('sid-2'));
    }

    public function test_read_returns_empty_string_when_session_missing(): void
    {
        $this->context->client->fetchReturn = null;
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        static::assertSame('', $handler->read('sid-3'));
    }

    public function test_read_returns_session_data_when_alive(): void
    {
        $this->context->client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => \time() + 3600];
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        static::assertSame('payload', $handler->read('sid-4'));
    }

    public function test_read_throws_when_data_is_not_string(): void
    {
        $this->context->client->fetchReturn = ['sess_data' => 123, 'sess_lifetime' => \time() + 3600];
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        $this->expectException(SessionException::class);
        $this->expectExceptionMessage('Unexpected session row shape: field "sess_data"');

        $handler->read('sid-bad-data');
    }

    public function test_read_throws_when_lifetime_is_not_int(): void
    {
        $this->context->client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => 'not-a-number'];
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        $this->expectException(SessionException::class);
        $this->expectExceptionMessage('Unexpected session row shape: field "sess_lifetime"');

        $handler->read('sid-bad-lifetime');
    }

    public function test_update_timestamp_emits_update_only(): void
    {
        $this->context->client->executeReturn = 1;
        $handler = $this->context->handler();

        static::assertTrue($handler->updateTimestamp('sid-8', 'unused'));
        static::assertCount(1, $this->context->client->executedQueries);
        static::assertStringContainsString('UPDATE', $this->context->client->executedQueries[0]['sql']);
        static::assertStringNotContainsString('INSERT', $this->context->client->executedQueries[0]['sql']);
    }

    public function test_write_emits_upsert_with_payload(): void
    {
        $this->context->client->executeReturn = 1;
        $handler = $this->context->handler(['ttl' => 600]);

        $inserts = $this->context->onlyQueries('INSERT', static function () use ($handler): void {
            $handler->write('sid-6', 'serialized-payload');
        });

        static::assertCount(1, $inserts);
        static::assertStringContainsString('ON CONFLICT', $inserts[0]['sql']);
        static::assertSame('sid-6', $inserts[0]['parameters'][0]);
    }
}
