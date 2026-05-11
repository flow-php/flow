<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLMessenger\Connection;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Exception\TransportException;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit\Double\SpyClient;
use PHPUnit\Framework\TestCase;

final class ConnectionTest extends TestCase
{
    public function test_ack_deletes_message_by_id_and_returns_true_when_row_affected(): void
    {
        $client = new SpyClient();
        $client->executeReturn = 1;
        $connection = new Connection($client);

        static::assertTrue($connection->ack('42'));
        static::assertCount(1, $client->executedQueries);
        static::assertSame('DELETE FROM public.messenger_messages WHERE id = $1', $client->executedQueries[0]['sql']);
        static::assertSame([42], $client->executedQueries[0]['parameters']);
    }

    public function test_ack_returns_false_when_no_row_affected(): void
    {
        $client = new SpyClient();
        $client->executeReturn = 0;
        $connection = new Connection($client);

        static::assertFalse($connection->ack('42'));
    }

    public function test_find_all_with_limit(): void
    {
        $client = new SpyClient();
        $client->fetchAllReturn = [
            ['id' => 1, 'body' => 'a', 'headers' => '{}'],
            ['id' => 2, 'body' => 'b', 'headers' => '{}'],
        ];
        $connection = new Connection($client);

        $rows = $connection->findAll(10);

        static::assertCount(2, $rows);
        static::assertSame(
            'SELECT * FROM public.messenger_messages WHERE queue_name = $1 ORDER BY available_at ASC LIMIT 10',
            $client->executedQueries[0]['sql'],
        );
        static::assertSame(['default'], $client->executedQueries[0]['parameters']);
    }

    public function test_find_all_without_limit(): void
    {
        $client = new SpyClient();
        $connection = new Connection($client);

        $connection->findAll();

        static::assertSame(
            'SELECT * FROM public.messenger_messages WHERE queue_name = $1 ORDER BY available_at ASC',
            $client->executedQueries[0]['sql'],
        );
    }

    public function test_find_by_id_filters_by_id_and_queue(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 5, 'body' => 'hello', 'headers' => '{}'];
        $connection = new Connection($client);

        $row = $connection->find('5');

        static::assertIsArray($row);
        static::assertSame(5, $row['id']);
        static::assertSame(
            'SELECT * FROM public.messenger_messages WHERE id = $1 AND queue_name = $2',
            $client->executedQueries[0]['sql'],
        );
        static::assertSame([5, 'default'], $client->executedQueries[0]['parameters']);
    }

    public function test_find_returns_null_when_not_found(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = null;
        $connection = new Connection($client);

        static::assertNull($connection->find('999'));
    }

    public function test_get_fetches_and_marks_delivered(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 7, 'body' => 'hello', 'headers' => '{}'];
        $connection = new Connection($client);

        $row = $connection->get();

        static::assertIsArray($row);
        static::assertSame(7, $row['id']);
        static::assertSame(1, $client->transactionCallCount);
        static::assertCount(2, $client->executedQueries);
        static::assertStringContainsString('FOR UPDATE SKIP LOCKED', $client->executedQueries[0]['sql']);
        static::assertStringStartsWith(
            'UPDATE public.messenger_messages SET delivered_at',
            $client->executedQueries[1]['sql'],
        );
    }

    public function test_get_message_count_filters_by_queue_and_availability(): void
    {
        $client = new SpyClient();
        $client->fetchScalarIntReturn = 42;
        $connection = new Connection($client);

        static::assertSame(42, $connection->getMessageCount());
        static::assertSame(
            'SELECT count(*) FROM public.messenger_messages WHERE queue_name = $1 AND available_at <= $2 AND delivered_at IS NULL',
            $client->executedQueries[0]['sql'],
        );
    }

    public function test_get_returns_null_when_no_messages_and_does_not_update(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = null;
        $connection = new Connection($client);

        static::assertNull($connection->get());
        static::assertSame(1, $client->transactionCallCount);
        static::assertCount(1, $client->executedQueries);
    }

    public function test_get_throws_on_unexpected_row_shape(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => null, 'body' => 'x', 'headers' => '{}'];
        $connection = new Connection($client);

        $this->expectException(TransportException::class);

        $connection->get();
    }

    public function test_keepalive_allows_seconds_equal_or_smaller_than_redeliver_timeout(): void
    {
        $client = new SpyClient();
        $connection = new Connection($client, redeliverTimeout: 3600);

        $connection->keepalive('123', seconds: 60);

        static::assertCount(1, $client->executedQueries);
    }

    public function test_keepalive_refreshes_delivered_at(): void
    {
        $client = new SpyClient();
        $connection = new Connection($client);

        $connection->keepalive('123');

        static::assertCount(1, $client->executedQueries);
        static::assertSame(
            'UPDATE public.messenger_messages SET delivered_at = $1 WHERE id = $2',
            $client->executedQueries[0]['sql'],
        );
        static::assertSame(123, $client->executedQueries[0]['parameters'][1]);
    }

    public function test_keepalive_throws_when_seconds_larger_than_redeliver_timeout(): void
    {
        $client = new SpyClient();
        $connection = new Connection($client, redeliverTimeout: 60);

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('cannot be smaller than the keepalive interval');

        $connection->keepalive('123', seconds: 120);
    }

    public function test_reject_deletes_message_by_id(): void
    {
        $client = new SpyClient();
        $client->executeReturn = 1;
        $connection = new Connection($client);

        static::assertTrue($connection->reject('42'));
        static::assertSame('DELETE FROM public.messenger_messages WHERE id = $1', $client->executedQueries[0]['sql']);
        static::assertSame([42], $client->executedQueries[0]['parameters']);
    }

    public function test_send_inserts_message_and_returns_id(): void
    {
        $client = new SpyClient();
        $client->fetchSingleReturn = ['id' => 100];
        $connection = new Connection($client);

        $id = $connection->send('hello', ['type' => 'App\\Foo']);

        static::assertSame('100', $id);
        static::assertCount(1, $client->executedQueries);
        static::assertSame(
            'INSERT INTO public.messenger_messages (body, headers, queue_name, created_at, available_at) VALUES ($1, $2, $3, $4, $5) RETURNING id',
            $client->executedQueries[0]['sql'],
        );
        static::assertSame('hello', $client->executedQueries[0]['parameters'][0]);
        static::assertSame('{"type":"App\\\\Foo"}', $client->executedQueries[0]['parameters'][1]);
        static::assertSame('default', $client->executedQueries[0]['parameters'][2]);
    }

    public function test_send_returns_string_id_from_int_return(): void
    {
        $client = new SpyClient();
        $client->fetchSingleReturn = ['id' => 42];
        $connection = new Connection($client);

        static::assertSame('42', $connection->send('body', []));
    }

    public function test_send_returns_string_id_from_string_return(): void
    {
        $client = new SpyClient();
        $client->fetchSingleReturn = ['id' => '99999999999999999'];
        $connection = new Connection($client);

        static::assertSame('99999999999999999', $connection->send('body', []));
    }

    public function test_send_throws_on_unexpected_row_shape(): void
    {
        $client = new SpyClient();
        $client->fetchSingleReturn = ['id' => null];
        $connection = new Connection($client);

        $this->expectException(TransportException::class);

        $connection->send('body', []);
    }

    public function test_send_with_delay_offsets_available_at(): void
    {
        $client = new SpyClient();
        $client->fetchSingleReturn = ['id' => 1];
        $connection = new Connection($client);

        $connection->send('body', [], 5000);

        static::assertCount(1, $client->executedQueries);

        // Position 3 = created_at, position 4 = available_at
        // available_at should be created_at + 5 seconds
    }

    public function test_uses_custom_queue_name(): void
    {
        $client = new SpyClient();
        $client->fetchAllReturn = [];
        $connection = new Connection($client, queueName: 'high_priority');

        $connection->findAll();

        static::assertSame(['high_priority'], $client->executedQueries[0]['parameters']);
    }

    public function test_uses_custom_table_and_schema(): void
    {
        $client = new SpyClient();
        $connection = new Connection($client, tableName: 'custom_queue', schemaName: 'app');

        $connection->ack('1');

        static::assertSame('DELETE FROM app.custom_queue WHERE id = $1', $client->executedQueries[0]['sql']);
    }
}
