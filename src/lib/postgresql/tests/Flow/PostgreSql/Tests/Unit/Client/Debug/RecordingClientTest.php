<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Debug;

use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\Debug\QueryLog;
use Flow\PostgreSql\Client\Debug\RecordingClient;
use Flow\PostgreSql\Client\Exception\PostgreSqlError;
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Tests\Mother\FakeClient;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(RecordingClient::class)]
final class RecordingClientTest extends TestCase
{
    public function test_execute_records_statement_parameters_and_affected_rows(): void
    {
        $log = new QueryLog();
        $inner = new FakeClient();
        $inner->executeReturn = 3;

        $affected = (new RecordingClient($inner, $log))->execute('DELETE FROM users WHERE id = $1', [42]);

        static::assertSame(3, $affected);
        static::assertCount(1, $log->queries());
        $query = $log->queries()[0];
        static::assertSame('DELETE FROM users WHERE id = $1', $query->sql);
        static::assertSame([42], $query->parameters);
        static::assertSame(3, $query->rowCount);
        static::assertFalse($query->failed);
        static::assertNull($query->error);
        static::assertGreaterThanOrEqual(0.0, $query->durationMs);
        static::assertSame('default', $query->connection);
        // Caller is captured and points outside the library decorator (lib frames are skipped).
        static::assertNotNull($query->caller);
        static::assertStringNotContainsString('Debug/RecordingClient.php', $query->caller);
    }

    public function test_records_the_configured_connection_name(): void
    {
        $log = new QueryLog();

        (new RecordingClient(new FakeClient(), $log, 'analytics'))->execute('SELECT 1');

        static::assertSame('analytics', $log->queries()[0]->connection);
    }

    public function test_fetch_records_one_row_when_row_returned(): void
    {
        $log = new QueryLog();
        $inner = new FakeClient();
        $inner->fetchReturn = ['id' => 1];

        $row = (new RecordingClient($inner, $log))->fetch('SELECT * FROM users LIMIT 1');

        static::assertSame(['id' => 1], $row);
        static::assertSame(1, $log->queries()[0]->rowCount);
    }

    public function test_fetch_records_zero_rows_when_null(): void
    {
        $log = new QueryLog();
        $inner = new FakeClient();
        $inner->fetchReturn = null;

        (new RecordingClient($inner, $log))->fetch('SELECT * FROM users WHERE 1 = 0');

        static::assertSame(0, $log->queries()[0]->rowCount);
    }

    public function test_fetch_all_records_returned_row_count(): void
    {
        $log = new QueryLog();
        $inner = new FakeClient();
        $inner->fetchAllReturn = [['id' => 1], ['id' => 2], ['id' => 3]];

        $rows = (new RecordingClient($inner, $log))->fetchAll('SELECT * FROM users');

        static::assertCount(3, $rows);
        static::assertSame(3, $log->queries()[0]->rowCount);
    }

    public function test_fetch_scalar_int_records_query(): void
    {
        $log = new QueryLog();
        $inner = new FakeClient();
        $inner->fetchScalarIntReturn = 7;

        $count = (new RecordingClient($inner, $log))->fetchScalarInt('SELECT COUNT(*) FROM users');

        static::assertSame(7, $count);
        static::assertSame('SELECT COUNT(*) FROM users', $log->queries()[0]->sql);
        static::assertSame(1, $log->queries()[0]->rowCount);
    }

    public function test_sql_object_is_recorded_via_to_sql(): void
    {
        $log = new QueryLog();
        $sql = new class implements Sql {
            public function toSql(): string
            {
                return 'SELECT 1';
            }
        };

        (new RecordingClient(new FakeClient(), $log))->execute($sql);

        static::assertSame('SELECT 1', $log->queries()[0]->sql);
    }

    public function test_failed_query_records_failure_and_rethrows(): void
    {
        $log = new QueryLog();
        $inner = new FakeClient();
        $inner->failNextQuery(QueryException::executionFailed('SELECT bad', PostgreSqlError::unknown('boom')));
        $client = new RecordingClient($inner, $log);
        $thrown = null;

        try {
            $client->fetch('SELECT bad');
            static::fail('Expected QueryException to be re-thrown');
        } catch (QueryException $e) {
            $thrown = $e;
        }

        $query = $log->queries()[0];
        static::assertTrue($query->failed);
        static::assertSame('SELECT bad', $query->sql);
        static::assertSame($thrown->getMessage(), $query->error);
        static::assertNull($query->rowCount);
    }

    public function test_cursor_records_statement_with_unknown_row_count_and_returns_inner_cursor(): void
    {
        $log = new QueryLog();
        $cursor = $this->createMock(Cursor::class);
        $inner = new FakeClient($cursor);

        $returned = (new RecordingClient($inner, $log))->cursor('SELECT * FROM big_table');

        static::assertSame($cursor, $returned);
        static::assertCount(1, $log->queries());
        static::assertSame('SELECT * FROM big_table', $log->queries()[0]->sql);
        static::assertNull($log->queries()[0]->rowCount);
    }

    public function test_transaction_control_and_connection_methods_delegate_without_recording(): void
    {
        $log = new QueryLog();
        $inner = new FakeClient();
        $client = new RecordingClient($inner, $log);

        $client->beginTransaction();
        $client->commit();
        $client->rollBack();
        $client->setAutoCommit(false);
        static::assertTrue($client->isConnected());

        static::assertSame([], $log->queries());
        static::assertSame(
            ['beginTransaction', 'commit', 'rollBack', 'setAutoCommit', 'isConnected'],
            $inner->delegated,
        );
    }
}
