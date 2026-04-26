<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLSession\Exception\SessionException;
use Flow\Bridge\Symfony\PostgreSQLSession\FlowPostgreSqlSessionHandler;
use Flow\Bridge\Symfony\PostgreSQLSession\Tests\Unit\Double\{SpyClient, TestableSessionHandler};
use PHPUnit\Framework\TestCase;

final class FlowPostgreSqlSessionHandlerTest extends TestCase
{
    public function test_close_after_gc_issues_delete_by_lifetime() : void
    {
        $client = new SpyClient();
        $handler = new TestableSessionHandler($client);

        $handler->gc(0);
        $handler->close();

        self::assertCount(1, $client->executedQueries);
        $closeQuery = $client->executedQueries[0];
        self::assertStringContainsString('DELETE FROM', $closeQuery['sql']);
        self::assertStringContainsString('sess_lifetime', $closeQuery['sql']);
    }

    public function test_close_without_gc_does_not_delete() : void
    {
        $client = new SpyClient();
        $handler = new TestableSessionHandler($client);

        $handler->close();

        self::assertSame([], $client->executedQueries);
    }

    public function test_constructor_accepts_advisory_lock_mode() : void
    {
        $handler = new TestableSessionHandler(
            new SpyClient(),
            ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_ADVISORY],
        );

        self::assertInstanceOf(FlowPostgreSqlSessionHandler::class, $handler);
    }

    public function test_constructor_accepts_none_lock_mode() : void
    {
        $handler = new TestableSessionHandler(
            new SpyClient(),
            ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE],
        );

        self::assertInstanceOf(FlowPostgreSqlSessionHandler::class, $handler);
    }

    public function test_constructor_rejects_invalid_lock_mode() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid lock_mode "99"');

        new TestableSessionHandler(new SpyClient(), ['lock_mode' => 99]);
    }

    public function test_do_destroy_commits_open_transaction_and_releases_lock() : void
    {
        $client = new SpyClient();
        $handler = new TestableSessionHandler($client, ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_TRANSACTIONAL]);
        $client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => \PHP_INT_MAX];

        $handler->exposedDoRead('sid-9');
        $handler->exposedDoDestroy('sid-9');

        self::assertSame(['begin', 'commit'], $client->transactionEvents);
        self::assertSame(0, $client->transactionLevel);
    }

    public function test_do_destroy_emits_delete_by_id() : void
    {
        $client = new SpyClient();
        $handler = new TestableSessionHandler($client);

        $handler->exposedDoDestroy('sid-7');

        self::assertCount(1, $client->executedQueries);
        self::assertStringContainsString('DELETE FROM', $client->executedQueries[0]['sql']);
        self::assertStringContainsString('sess_id', $client->executedQueries[0]['sql']);
        self::assertSame(['sid-7'], $client->executedQueries[0]['parameters']);
    }

    public function test_do_read_advisory_mode_acquires_lock_before_select() : void
    {
        $client = new SpyClient();
        $handler = new TestableSessionHandler(
            $client,
            ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_ADVISORY],
        );

        $handler->exposedDoRead('sid-5');

        self::assertGreaterThanOrEqual(2, \count($client->executedQueries));
        self::assertStringContainsString('pg_advisory_lock', $client->executedQueries[0]['sql']);
        self::assertStringContainsString('SELECT', $client->executedQueries[1]['sql']);
        self::assertStringContainsString('sess_data', $client->executedQueries[1]['sql']);
    }

    public function test_do_read_emits_for_update_in_transactional_mode() : void
    {
        $client = new SpyClient();
        $handler = new TestableSessionHandler(
            $client,
            ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_TRANSACTIONAL],
        );

        $handler->exposedDoRead('sid-1');

        self::assertSame(1, $client->transactionLevel);
        self::assertCount(1, $client->executedQueries);
        self::assertStringContainsString('FOR UPDATE', $client->executedQueries[0]['sql']);
    }

    public function test_do_read_returns_empty_string_when_session_expired() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => \time() - 100];
        $handler = new TestableSessionHandler($client, ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        self::assertSame('', $handler->exposedDoRead('sid-2'));
    }

    public function test_do_read_returns_empty_string_when_session_missing() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = null;
        $handler = new TestableSessionHandler($client, ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        self::assertSame('', $handler->exposedDoRead('sid-3'));
    }

    public function test_do_read_returns_session_data_when_alive() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => \time() + 3600];
        $handler = new TestableSessionHandler($client, ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        self::assertSame('payload', $handler->exposedDoRead('sid-4'));
    }

    public function test_do_read_throws_when_data_is_not_string() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['sess_data' => 123, 'sess_lifetime' => \time() + 3600];
        $handler = new TestableSessionHandler($client, ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        $this->expectException(SessionException::class);
        $this->expectExceptionMessage('Unexpected session row shape: field "sess_data"');

        $handler->exposedDoRead('sid-bad-data');
    }

    public function test_do_read_throws_when_lifetime_is_not_int() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['sess_data' => 'payload', 'sess_lifetime' => 'not-a-number'];
        $handler = new TestableSessionHandler($client, ['lock_mode' => FlowPostgreSqlSessionHandler::LOCK_NONE]);

        $this->expectException(SessionException::class);
        $this->expectExceptionMessage('Unexpected session row shape: field "sess_lifetime"');

        $handler->exposedDoRead('sid-bad-lifetime');
    }

    public function test_do_write_emits_upsert_with_payload() : void
    {
        $client = new SpyClient();
        $client->executeReturn = 1;
        $handler = new TestableSessionHandler(
            $client,
            ['ttl' => 600],
        );

        $handler->exposedDoWrite('sid-6', 'serialized-payload');

        self::assertCount(1, $client->executedQueries);
        $sql = $client->executedQueries[0]['sql'];
        self::assertStringContainsString('INSERT INTO', $sql);
        self::assertStringContainsString('ON CONFLICT', $sql);
        self::assertSame('sid-6', $client->executedQueries[0]['parameters'][0]);
    }

    public function test_gc_does_not_run_sql() : void
    {
        $client = new SpyClient();
        $handler = new TestableSessionHandler($client);

        $result = $handler->gc(0);

        self::assertSame(0, $result);
        self::assertSame([], $client->executedQueries);
    }

    public function test_purge_all_emits_truncate() : void
    {
        $client = new SpyClient();
        $handler = new TestableSessionHandler($client);

        $count = $handler->purgeAll();

        self::assertSame(0, $count);
        self::assertCount(1, $client->executedQueries);
        self::assertStringContainsString('TRUNCATE', $client->executedQueries[0]['sql']);
    }

    public function test_purge_expired_returns_affected_row_count() : void
    {
        $client = new SpyClient();
        $client->executeReturn = 7;
        $handler = new TestableSessionHandler($client);

        self::assertSame(7, $handler->purgeExpired());
        self::assertStringContainsString('DELETE FROM', $client->executedQueries[0]['sql']);
        self::assertStringContainsString('sess_lifetime', $client->executedQueries[0]['sql']);
    }

    public function test_update_timestamp_emits_update_only() : void
    {
        $client = new SpyClient();
        $client->executeReturn = 1;
        $handler = new TestableSessionHandler($client);

        self::assertTrue($handler->updateTimestamp('sid-8', 'unused'));
        self::assertCount(1, $client->executedQueries);
        self::assertStringContainsString('UPDATE', $client->executedQueries[0]['sql']);
        self::assertStringNotContainsString('INSERT', $client->executedQueries[0]['sql']);
    }
}
