<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Profiler;

use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\ProfilerClient;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\QueryRecorder;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Double\FakeClient;
use Flow\PostgreSql\Client\ConvertedParameters;
use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\Exception\PostgreSqlError;
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\QueryBuilder\Sql;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(ProfilerClient::class)]
final class ProfilerClientTest extends TestCase
{
    public function test_execute_records_statement_parameters_and_affected_rows(): void
    {
        $recorder = new QueryRecorder();
        $inner = new FakeClient();
        $inner->executeReturn = 3;

        $affected = (new ProfilerClient($inner, $recorder))->execute('DELETE FROM users WHERE id = $1', [42]);

        static::assertSame(3, $affected);
        static::assertCount(1, $recorder->queries());
        $query = $recorder->queries()[0];
        static::assertSame('DELETE FROM users WHERE id = $1', $query->sql);
        static::assertSame([42], $query->parameters);
        static::assertSame(3, $query->rowCount);
        static::assertFalse($query->failed);
        static::assertNull($query->error);
        static::assertGreaterThanOrEqual(0.0, $query->durationMs);
        static::assertSame('default', $query->connection);
        // Caller is captured and points outside the library decorator (lib frames are skipped).
        static::assertNotNull($query->caller);
        static::assertStringNotContainsString('Debug/ProfilerClient.php', $query->caller);
    }

    public function test_execute_records_converted_parameters_as_their_values(): void
    {
        $recorder = new QueryRecorder();
        $inner = new FakeClient();
        $inner->executeReturn = 2;

        $affected = (new ProfilerClient($inner, $recorder))->execute(
            'DELETE FROM users WHERE id = $1',
            new ConvertedParameters(['42']),
        );

        static::assertSame(2, $affected);
        static::assertCount(1, $recorder->queries());
        static::assertSame('DELETE FROM users WHERE id = $1', $recorder->queries()[0]->sql);
        static::assertSame(['42'], $recorder->queries()[0]->parameters);
        static::assertSame(2, $recorder->queries()[0]->rowCount);
    }

    public function test_records_the_configured_connection_name(): void
    {
        $recorder = new QueryRecorder();

        (new ProfilerClient(new FakeClient(), $recorder, 'analytics'))->execute('SELECT 1');

        static::assertSame('analytics', $recorder->queries()[0]->connection);
    }

    public function test_every_statement_bearing_method_is_recorded(): void
    {
        $recorder = new QueryRecorder();
        $client = new ProfilerClient(new FakeClient($this->createStub(Cursor::class)), $recorder);
        $mapper = $this->createStub(RowMapper::class);

        $client->execute('UPDATE t SET a = 1');
        $client->explain('SELECT 1');
        $client->fetch('SELECT 1');
        $client->fetchAll('SELECT 1');
        $client->fetchAllInto($mapper, 'SELECT 1');
        $client->fetchInto($mapper, 'SELECT 1');
        $client->fetchOne('SELECT 1');
        $client->fetchOneInto($mapper, 'SELECT 1');
        $client->fetchScalar('SELECT 1');
        $client->fetchScalarBool('SELECT 1');
        $client->fetchScalarFloat('SELECT 1');
        $client->fetchScalarInt('SELECT 1');
        $client->fetchScalarString('SELECT 1');
        $client->fetchSingle('SELECT 1');
        $client->fetchSingleInto($mapper, 'SELECT 1');
        $client->cursor('SELECT 1');
        $client->describe('SELECT 1');

        static::assertCount(17, $recorder->queries());
    }

    public function test_failed_cursor_records_failure_and_rethrows(): void
    {
        $recorder = new QueryRecorder();
        $inner = new FakeClient($this->createStub(Cursor::class));
        $inner->failNextQuery(QueryException::executionFailed('SELECT bad', PostgreSqlError::unknown('boom')));
        $client = new ProfilerClient($inner, $recorder);

        try {
            $client->cursor('SELECT bad');
            static::fail('Expected QueryException to be re-thrown');
        } catch (QueryException) {
            // expected
        }

        static::assertTrue($recorder->queries()[0]->failed);
        static::assertNull($recorder->queries()[0]->rowCount);
    }

    public function test_delegation_methods_forward_without_recording(): void
    {
        $recorder = new QueryRecorder();
        $inner = new FakeClient();
        $client = new ProfilerClient($inner, $recorder);

        $client->beginTransaction();
        $client->commit();
        $client->rollBack();
        $client->setAutoCommit(true);
        $client->close();
        $client->converters();
        $client->parameters();
        $client->listen('channel');
        $client->unlisten('channel');
        $client->wait(0);

        static::assertSame(0, $client->getTransactionNestingLevel());
        static::assertTrue($client->isAutoCommit());
        static::assertTrue($client->isConnected());
        static::assertSame(0, $client->lastInsertId('seq'));
        static::assertSame('result', $client->transaction(static fn(): string => 'result'));

        static::assertSame([], $recorder->queries());
    }

    public function test_fetch_records_one_row_when_row_returned(): void
    {
        $recorder = new QueryRecorder();
        $inner = new FakeClient();
        $inner->fetchReturn = ['id' => 1];

        $row = (new ProfilerClient($inner, $recorder))->fetch('SELECT * FROM users LIMIT 1');

        static::assertSame(['id' => 1], $row);
        static::assertSame(1, $recorder->queries()[0]->rowCount);
    }

    public function test_fetch_records_zero_rows_when_null(): void
    {
        $recorder = new QueryRecorder();
        $inner = new FakeClient();
        $inner->fetchReturn = null;

        (new ProfilerClient($inner, $recorder))->fetch('SELECT * FROM users WHERE 1 = 0');

        static::assertSame(0, $recorder->queries()[0]->rowCount);
    }

    public function test_fetch_all_records_returned_row_count(): void
    {
        $recorder = new QueryRecorder();
        $inner = new FakeClient();
        $inner->fetchAllReturn = [['id' => 1], ['id' => 2], ['id' => 3]];

        $rows = (new ProfilerClient($inner, $recorder))->fetchAll('SELECT * FROM users');

        static::assertCount(3, $rows);
        static::assertSame(3, $recorder->queries()[0]->rowCount);
    }

    public function test_fetch_scalar_int_records_query(): void
    {
        $recorder = new QueryRecorder();
        $inner = new FakeClient();
        $inner->fetchScalarIntReturn = 7;

        $count = (new ProfilerClient($inner, $recorder))->fetchScalarInt('SELECT COUNT(*) FROM users');

        static::assertSame(7, $count);
        static::assertSame('SELECT COUNT(*) FROM users', $recorder->queries()[0]->sql);
        static::assertSame(1, $recorder->queries()[0]->rowCount);
    }

    public function test_sql_object_is_recorded_via_to_sql(): void
    {
        $recorder = new QueryRecorder();
        $sql = new class implements Sql {
            public function toSql(): string
            {
                return 'SELECT 1';
            }
        };

        (new ProfilerClient(new FakeClient(), $recorder))->execute($sql);

        static::assertSame('SELECT 1', $recorder->queries()[0]->sql);
    }

    public function test_failed_query_records_failure_and_rethrows(): void
    {
        $recorder = new QueryRecorder();
        $inner = new FakeClient();
        $inner->failNextQuery(QueryException::executionFailed('SELECT bad', PostgreSqlError::unknown('boom')));
        $client = new ProfilerClient($inner, $recorder);
        $thrown = null;

        try {
            $client->fetch('SELECT bad');
            static::fail('Expected QueryException to be re-thrown');
        } catch (QueryException $e) {
            $thrown = $e;
        }

        $query = $recorder->queries()[0];
        static::assertTrue($query->failed);
        static::assertSame('SELECT bad', $query->sql);
        static::assertSame($thrown->getMessage(), $query->error);
        static::assertNull($query->rowCount);
    }

    public function test_cursor_records_statement_with_unknown_row_count_and_returns_inner_cursor(): void
    {
        $recorder = new QueryRecorder();
        $cursor = $this->createStub(Cursor::class);
        $inner = new FakeClient($cursor);

        $returned = (new ProfilerClient($inner, $recorder))->cursor('SELECT * FROM big_table');

        static::assertSame($cursor, $returned);
        static::assertCount(1, $recorder->queries());
        static::assertSame('SELECT * FROM big_table', $recorder->queries()[0]->sql);
        static::assertNull($recorder->queries()[0]->rowCount);
    }

    public function test_transaction_control_and_connection_methods_delegate_without_recording(): void
    {
        $recorder = new QueryRecorder();
        $inner = new FakeClient();
        $client = new ProfilerClient($inner, $recorder);

        $client->beginTransaction();
        $client->commit();
        $client->rollBack();
        $client->setAutoCommit(false);
        static::assertTrue($client->isConnected());

        static::assertSame([], $recorder->queries());
        static::assertSame(
            ['beginTransaction', 'commit', 'rollBack', 'setAutoCommit', 'isConnected'],
            $inner->delegated,
        );
    }
}
