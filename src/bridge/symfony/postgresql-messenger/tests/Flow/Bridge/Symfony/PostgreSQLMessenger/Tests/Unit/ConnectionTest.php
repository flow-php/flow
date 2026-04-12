<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLMessenger\Connection;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Exception\TransportException;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit\Double\SpyClient;
use PHPUnit\Framework\TestCase;

final class ConnectionTest extends TestCase
{
    public function test_ack_deletes_message_by_id_and_returns_true_when_row_affected() : void
    {
        $client = new SpyClient();
        $client->executeReturn = 1;
        $connection = new Connection($client);

        self::assertTrue($connection->ack('42'));
        self::assertCount(1, $client->executedQueries);
        self::assertSame('DELETE FROM public.messenger_messages WHERE id = $1', $client->executedQueries[0]['sql']);
        self::assertSame([42], $client->executedQueries[0]['parameters']);
    }

    public function test_ack_returns_false_when_no_row_affected() : void
    {
        $client = new SpyClient();
        $client->executeReturn = 0;
        $connection = new Connection($client);

        self::assertFalse($connection->ack('42'));
    }

    public function test_find_all_with_limit() : void
    {
        $client = new SpyClient();
        $client->fetchAllReturn = [
            ['id' => 1, 'body' => 'a', 'headers' => '{}'],
            ['id' => 2, 'body' => 'b', 'headers' => '{}'],
        ];
        $connection = new Connection($client);

        $rows = $connection->findAll(10);

        self::assertCount(2, $rows);
        self::assertSame('SELECT * FROM public.messenger_messages WHERE queue_name = $1 ORDER BY available_at ASC LIMIT 10', $client->executedQueries[0]['sql']);
        self::assertSame(['default'], $client->executedQueries[0]['parameters']);
    }

    public function test_find_all_without_limit() : void
    {
        $client = new SpyClient();
        $connection = new Connection($client);

        $connection->findAll();

        self::assertSame('SELECT * FROM public.messenger_messages WHERE queue_name = $1 ORDER BY available_at ASC', $client->executedQueries[0]['sql']);
    }

    public function test_find_by_id_filters_by_id_and_queue() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 5, 'body' => 'hello', 'headers' => '{}'];
        $connection = new Connection($client);

        $row = $connection->find('5');

        self::assertIsArray($row);
        self::assertSame(5, $row['id']);
        self::assertSame('SELECT * FROM public.messenger_messages WHERE id = $1 AND queue_name = $2', $client->executedQueries[0]['sql']);
        self::assertSame([5, 'default'], $client->executedQueries[0]['parameters']);
    }

    public function test_find_returns_null_when_not_found() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = null;
        $connection = new Connection($client);

        self::assertNull($connection->find('999'));
    }

    public function test_get_fetches_and_marks_delivered() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 7, 'body' => 'hello', 'headers' => '{}'];
        $connection = new Connection($client);

        $row = $connection->get();

        self::assertIsArray($row);
        self::assertSame(7, $row['id']);
        self::assertSame(1, $client->transactionCallCount);
        self::assertCount(2, $client->executedQueries);
        self::assertStringContainsString('FOR UPDATE SKIP LOCKED', $client->executedQueries[0]['sql']);
        self::assertStringStartsWith('UPDATE public.messenger_messages SET delivered_at', $client->executedQueries[1]['sql']);
    }

    public function test_get_message_count_filters_by_queue_and_availability() : void
    {
        $client = new SpyClient();
        $client->fetchScalarIntReturn = 42;
        $connection = new Connection($client);

        self::assertSame(42, $connection->getMessageCount());
        self::assertSame('SELECT count(*) FROM public.messenger_messages WHERE queue_name = $1 AND available_at <= $2 AND delivered_at IS NULL', $client->executedQueries[0]['sql']);
    }

    public function test_get_returns_null_when_no_messages_and_does_not_update() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = null;
        $connection = new Connection($client);

        self::assertNull($connection->get());
        self::assertSame(1, $client->transactionCallCount);
        self::assertCount(1, $client->executedQueries);
    }

    public function test_get_throws_on_unexpected_row_shape() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => null, 'body' => 'x', 'headers' => '{}'];
        $connection = new Connection($client);

        $this->expectException(TransportException::class);

        $connection->get();
    }

    public function test_keepalive_allows_seconds_equal_or_smaller_than_redeliver_timeout() : void
    {
        $client = new SpyClient();
        $connection = new Connection($client, redeliverTimeout: 3600);

        $connection->keepalive('123', seconds: 60);

        self::assertCount(1, $client->executedQueries);
    }

    public function test_keepalive_refreshes_delivered_at() : void
    {
        $client = new SpyClient();
        $connection = new Connection($client);

        $connection->keepalive('123');

        self::assertCount(1, $client->executedQueries);
        self::assertSame('UPDATE public.messenger_messages SET delivered_at = $1 WHERE id = $2', $client->executedQueries[0]['sql']);
        self::assertSame(123, $client->executedQueries[0]['parameters'][1]);
    }

    public function test_keepalive_throws_when_seconds_larger_than_redeliver_timeout() : void
    {
        $client = new SpyClient();
        $connection = new Connection($client, redeliverTimeout: 60);

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('cannot be smaller than the keepalive interval');

        $connection->keepalive('123', seconds: 120);
    }

    public function test_reject_deletes_message_by_id() : void
    {
        $client = new SpyClient();
        $client->executeReturn = 1;
        $connection = new Connection($client);

        self::assertTrue($connection->reject('42'));
        self::assertSame('DELETE FROM public.messenger_messages WHERE id = $1', $client->executedQueries[0]['sql']);
        self::assertSame([42], $client->executedQueries[0]['parameters']);
    }

    public function test_send_inserts_message_and_returns_id() : void
    {
        $client = new SpyClient();
        $client->fetchOneReturn = ['id' => 100];
        $connection = new Connection($client);

        $id = $connection->send('hello', ['type' => 'App\\Foo']);

        self::assertSame('100', $id);
        self::assertCount(1, $client->executedQueries);
        self::assertSame('INSERT INTO public.messenger_messages (body, headers, queue_name, created_at, available_at) VALUES ($1, $2, $3, $4, $5) RETURNING id', $client->executedQueries[0]['sql']);
        self::assertSame('hello', $client->executedQueries[0]['parameters'][0]);
        self::assertSame('{"type":"App\\\\Foo"}', $client->executedQueries[0]['parameters'][1]);
        self::assertSame('default', $client->executedQueries[0]['parameters'][2]);
    }

    public function test_send_returns_string_id_from_int_return() : void
    {
        $client = new SpyClient();
        $client->fetchOneReturn = ['id' => 42];
        $connection = new Connection($client);

        self::assertSame('42', $connection->send('body', []));
    }

    public function test_send_returns_string_id_from_string_return() : void
    {
        $client = new SpyClient();
        $client->fetchOneReturn = ['id' => '99999999999999999'];
        $connection = new Connection($client);

        self::assertSame('99999999999999999', $connection->send('body', []));
    }

    public function test_send_throws_on_unexpected_row_shape() : void
    {
        $client = new SpyClient();
        $client->fetchOneReturn = ['id' => null];
        $connection = new Connection($client);

        $this->expectException(TransportException::class);

        $connection->send('body', []);
    }

    public function test_send_with_delay_offsets_available_at() : void
    {
        $client = new SpyClient();
        $client->fetchOneReturn = ['id' => 1];
        $connection = new Connection($client);

        $connection->send('body', [], 5000);

        self::assertCount(1, $client->executedQueries);
        // Position 3 = created_at, position 4 = available_at
        // available_at should be created_at + 5 seconds
    }

    public function test_uses_custom_queue_name() : void
    {
        $client = new SpyClient();
        $client->fetchAllReturn = [];
        $connection = new Connection($client, queueName: 'high_priority');

        $connection->findAll();

        self::assertSame(['high_priority'], $client->executedQueries[0]['parameters']);
    }

    public function test_uses_custom_table_and_schema() : void
    {
        $client = new SpyClient();
        $connection = new Connection($client, tableName: 'custom_queue', schemaName: 'app');

        $connection->ack('1');

        self::assertSame('DELETE FROM app.custom_queue WHERE id = $1', $client->executedQueries[0]['sql']);
    }
}
