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

    protected function setUp() : void
    {
        $this->context = new PostgreSqlSessionContext();
    }

    public function test_close_after_gc_issues_delete_by_lifetime() : void
    {
        $handler = $this->context->handler();

        $handler->gc(0);
        $handler->close();

        self::assertCount(1, $this->context->client->executedQueries);
        $closeQuery = $this->context->client->executedQueries[0];
        self::assertStringContainsString('DELETE FROM', $closeQuery['sql']);
        self::assertStringContainsString('sess_lifetime', $closeQuery['sql']);
    }

    public function test_close_without_gc_does_not_delete() : void
    {
        $handler = $this->context->handler();

        $handler->close();

        self::assertSame([], $this->context->client->executedQueries);
    }

    public function test_constructor_accepts_advisory_lock_mode() : void
    {
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_ADVISORY]);

        self::assertInstanceOf(FlowPostgreSqlSessionHandler::class, $handler);
    }

    public function test_constructor_accepts_none_lock_mode() : void
    {
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        self::assertInstanceOf(FlowPostgreSqlSessionHandler::class, $handler);
    }

    public function test_constructor_rejects_invalid_lock_mode() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid lock_mode "99"');

        $this->context->handler(['lock_mode' => 99]);
    }

    public function test_destroy_commits_open_transaction_and_releases_lock() : void
    {
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_TRANSACTIONAL]);
        $this->context->client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => \PHP_INT_MAX];

        $handler->read('sid-9');
        $handler->destroy('sid-9');

        self::assertSame(['begin', 'commit'], $this->context->client->transactionEvents);
        self::assertSame(0, $this->context->client->transactionLevel);
    }

    public function test_destroy_emits_delete_by_id() : void
    {
        $handler = $this->context->handler();

        $deletes = $this->context->onlyQueries('DELETE', static function () use ($handler) : void {
            $handler->destroy('sid-7');
        });

        self::assertCount(1, $deletes);
        self::assertStringContainsString('sess_id', $deletes[0]['sql']);
        self::assertSame(['sid-7'], $deletes[0]['parameters']);
    }

    public function test_gc_does_not_run_sql() : void
    {
        $handler = $this->context->handler();

        $result = $handler->gc(0);

        self::assertSame(0, $result);
        self::assertSame([], $this->context->client->executedQueries);
    }

    public function test_purge_all_emits_truncate() : void
    {
        $handler = $this->context->handler();

        $count = $handler->purgeAll();

        self::assertSame(0, $count);
        self::assertCount(1, $this->context->client->executedQueries);
        self::assertStringContainsString('TRUNCATE', $this->context->client->executedQueries[0]['sql']);
    }

    public function test_purge_expired_returns_affected_row_count() : void
    {
        $this->context->client->executeReturn = 7;
        $handler = $this->context->handler();

        self::assertSame(7, $handler->purgeExpired());
        self::assertStringContainsString('DELETE FROM', $this->context->client->executedQueries[0]['sql']);
        self::assertStringContainsString('sess_lifetime', $this->context->client->executedQueries[0]['sql']);
    }

    public function test_read_advisory_mode_acquires_lock_before_select() : void
    {
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_ADVISORY]);

        $handler->read('sid-5');

        self::assertGreaterThanOrEqual(2, \count($this->context->client->executedQueries));
        self::assertStringContainsString('pg_advisory_lock', $this->context->client->executedQueries[0]['sql']);
        self::assertStringContainsString('SELECT', $this->context->client->executedQueries[1]['sql']);
        self::assertStringContainsString('sess_data', $this->context->client->executedQueries[1]['sql']);
    }

    public function test_read_emits_for_update_in_transactional_mode() : void
    {
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_TRANSACTIONAL]);

        $handler->read('sid-1');

        self::assertSame(1, $this->context->client->transactionLevel);
        self::assertCount(1, $this->context->client->executedQueries);
        self::assertStringContainsString('FOR UPDATE', $this->context->client->executedQueries[0]['sql']);
    }

    public function test_read_returns_empty_string_when_session_expired() : void
    {
        $this->context->client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => \time() - 100];
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        self::assertSame('', $handler->read('sid-2'));
    }

    public function test_read_returns_empty_string_when_session_missing() : void
    {
        $this->context->client->fetchReturn = null;
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        self::assertSame('', $handler->read('sid-3'));
    }

    public function test_read_returns_session_data_when_alive() : void
    {
        $this->context->client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => \time() + 3600];
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        self::assertSame('payload', $handler->read('sid-4'));
    }

    public function test_read_throws_when_data_is_not_string() : void
    {
        $this->context->client->fetchReturn = ['sess_data' => 123, 'sess_lifetime' => \time() + 3600];
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        $this->expectException(SessionException::class);
        $this->expectExceptionMessage('Unexpected session row shape: field "sess_data"');

        $handler->read('sid-bad-data');
    }

    public function test_read_throws_when_lifetime_is_not_int() : void
    {
        $this->context->client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => 'not-a-number'];
        $handler = $this->context->handler(['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        $this->expectException(SessionException::class);
        $this->expectExceptionMessage('Unexpected session row shape: field "sess_lifetime"');

        $handler->read('sid-bad-lifetime');
    }

    public function test_update_timestamp_emits_update_only() : void
    {
        $this->context->client->executeReturn = 1;
        $handler = $this->context->handler();

        self::assertTrue($handler->updateTimestamp('sid-8', 'unused'));
        self::assertCount(1, $this->context->client->executedQueries);
        self::assertStringContainsString('UPDATE', $this->context->client->executedQueries[0]['sql']);
        self::assertStringNotContainsString('INSERT', $this->context->client->executedQueries[0]['sql']);
    }

    public function test_write_emits_upsert_with_payload() : void
    {
        $this->context->client->executeReturn = 1;
        $handler = $this->context->handler(['ttl' => 600]);

        $inserts = $this->context->onlyQueries('INSERT', static function () use ($handler) : void {
            $handler->write('sid-6', 'serialized-payload');
        });

        self::assertCount(1, $inserts);
        self::assertStringContainsString('ON CONFLICT', $inserts[0]['sql']);
        self::assertSame('sid-6', $inserts[0]['parameters'][0]);
    }
}
