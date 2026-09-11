<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLMessenger\Connection;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Exception\TransportException as BridgeTransportException;
use Flow\Bridge\Symfony\PostgreSQLMessenger\FlowPostgreSqlReceiver;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit\Double\FakeSerializer;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit\Double\SpyClient;
use Flow\PostgreSql\Client\ConvertedParameters;
use Flow\PostgreSql\QueryBuilder\Sql;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;

use function iterator_to_array;

final class FlowPostgreSqlReceiverTest extends TestCase
{
    public function test_ack_delegates_to_connection_with_stamp_id(): void
    {
        $client = new SpyClient();
        $client->executeReturn = 1;
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $envelope = (new Envelope((object) []))->with(new TransportMessageIdStamp('42'));
        $receiver->ack($envelope);

        static::assertSame([42], $client->executedQueries[0]['parameters']);
    }

    public function test_ack_throws_transport_exception_without_stamp(): void
    {
        $receiver = new FlowPostgreSqlReceiver(new Connection(new SpyClient()), new FakeSerializer());

        $this->expectException(TransportException::class);

        $receiver->ack(new Envelope((object) []));
    }

    public function test_all_with_limit_passes_limit_to_connection(): void
    {
        $client = new SpyClient();
        $client->fetchAllReturn = [];
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        iterator_to_array($receiver->all(5), false);

        static::assertStringContainsString('LIMIT 5', $client->executedQueries[0]['sql']);
    }

    public function test_all_without_limit_returns_decoded_envelopes(): void
    {
        $client = new SpyClient();
        $client->fetchAllReturn = [
            ['id' => 1, 'body' => 'a', 'headers' => '{}'],
            ['id' => 2, 'body' => 'b', 'headers' => '{}'],
        ];
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $envelopes = iterator_to_array($receiver->all(), false);

        static::assertCount(2, $envelopes);
        $stamp0 = $envelopes[0]->last(TransportMessageIdStamp::class);
        static::assertNotNull($stamp0);
        static::assertSame('1', $stamp0->getId());
        $stamp1 = $envelopes[1]->last(TransportMessageIdStamp::class);
        static::assertNotNull($stamp1);
        static::assertSame('2', $stamp1->getId());
    }

    public function test_all_wraps_connection_exceptions(): void
    {
        $client = new class() extends SpyClient {
            public function fetchAll(Sql|string $sql, array $parameters = []): array
            {
                throw new RuntimeException('db failed');
            }
        };
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $this->expectException(TransportException::class);
        iterator_to_array($receiver->all(), false);
    }

    public function test_find_returns_decoded_envelope_with_stamp(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 5, 'body' => 'hello', 'headers' => '{"type":"App\\\\Foo"}'];
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $envelope = $receiver->find('5');

        static::assertNotNull($envelope);
        $stamp = $envelope->last(TransportMessageIdStamp::class);
        static::assertNotNull($stamp);
        static::assertSame('5', $stamp->getId());
    }

    public function test_find_returns_null_when_not_found(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = null;
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        static::assertNull($receiver->find('999'));
    }

    public function test_find_throws_on_unexpected_id_shape(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => null, 'body' => 'x', 'headers' => '{}'];
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $this->expectException(BridgeTransportException::class);

        $receiver->find('1');
    }

    public function test_find_wraps_connection_exceptions(): void
    {
        $client = new class() extends SpyClient {
            public function fetch(Sql|string $sql, array $parameters = []): ?array
            {
                throw new RuntimeException('db failed');
            }
        };
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $this->expectException(TransportException::class);
        $receiver->find('1');
    }

    public function test_get_decodes_row_and_adds_stamps(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 7, 'body' => 'hello', 'headers' => '{"type":"App\\\\Foo"}'];
        $serializer = new FakeSerializer();
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), $serializer);

        $envelopes = iterator_to_array($receiver->get(), false);

        static::assertCount(1, $envelopes);
        static::assertCount(1, $serializer->decodeCalls);
        static::assertSame('hello', $serializer->decodeCalls[0]['body']);
        static::assertSame(['type' => 'App\\Foo'], $serializer->decodeCalls[0]['headers']);
        $stamp = $envelopes[0]->last(TransportMessageIdStamp::class);
        static::assertNotNull($stamp);
        static::assertSame('7', $stamp->getId());
    }

    public function test_get_message_count_delegates_to_connection(): void
    {
        $client = new SpyClient();
        $client->fetchScalarIntReturn = 99;
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        static::assertSame(99, $receiver->getMessageCount());
    }

    public function test_get_message_count_wraps_exceptions(): void
    {
        $client = new class() extends SpyClient {
            public function fetchScalarInt(Sql|string $sql, array $parameters = []): int
            {
                throw new RuntimeException('db down');
            }
        };
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $this->expectException(TransportException::class);
        $receiver->getMessageCount();
    }

    public function test_get_returns_empty_when_no_messages(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = null;
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        static::assertSame([], iterator_to_array($receiver->get(), false));
    }

    public function test_get_throws_on_malformed_headers_json(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 1, 'body' => 'x', 'headers' => 'not json'];
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $this->expectException(MessageDecodingFailedException::class);

        iterator_to_array($receiver->get(), false);
    }

    public function test_get_throws_on_non_array_headers(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 1, 'body' => 'x', 'headers' => '"plain-string"'];
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $this->expectException(MessageDecodingFailedException::class);

        iterator_to_array($receiver->get(), false);
    }

    public function test_get_throws_on_unexpected_body_shape(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 1, 'body' => 123, 'headers' => '{}'];
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $this->expectException(BridgeTransportException::class);

        iterator_to_array($receiver->get(), false);
    }

    public function test_get_throws_on_unexpected_headers_shape(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 1, 'body' => 'x', 'headers' => 123];
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $this->expectException(BridgeTransportException::class);

        iterator_to_array($receiver->get(), false);
    }

    public function test_get_throws_on_unexpected_id_shape(): void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => null, 'body' => 'x', 'headers' => '{}'];
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $this->expectException(TransportException::class);

        iterator_to_array($receiver->get(), false);
    }

    public function test_get_wraps_connection_exceptions(): void
    {
        $client = new class() extends SpyClient {
            public function transaction(callable $callback): mixed
            {
                throw new RuntimeException('tx failed');
            }
        };
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $this->expectException(TransportException::class);
        iterator_to_array($receiver->get(), false);
    }

    public function test_keepalive_delegates_with_stamp_id_and_seconds(): void
    {
        $client = new SpyClient();
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $envelope = (new Envelope((object) []))->with(new TransportMessageIdStamp('42'));
        $receiver->keepalive($envelope, 60);

        static::assertSame([42], [$client->executedQueries[0]['parameters'][1]]);
    }

    public function test_keepalive_wraps_exceptions(): void
    {
        $client = new class() extends SpyClient {
            public function execute(Sql|string $sql, array|ConvertedParameters $parameters = []): int
            {
                throw new RuntimeException('oops');
            }
        };
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $envelope = (new Envelope((object) []))->with(new TransportMessageIdStamp('1'));

        $this->expectException(TransportException::class);
        $receiver->keepalive($envelope);
    }

    public function test_reject_delegates_to_connection(): void
    {
        $client = new SpyClient();
        $client->executeReturn = 1;
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $envelope = (new Envelope((object) []))->with(new TransportMessageIdStamp('42'));
        $receiver->reject($envelope);

        static::assertSame([42], $client->executedQueries[0]['parameters']);
    }

    public function test_reject_wraps_connection_exceptions(): void
    {
        $client = new class() extends SpyClient {
            public function execute(Sql|string $sql, array|ConvertedParameters $parameters = []): int
            {
                throw new RuntimeException('delete failed');
            }
        };
        $receiver = new FlowPostgreSqlReceiver(new Connection($client), new FakeSerializer());

        $envelope = (new Envelope((object) []))->with(new TransportMessageIdStamp('1'));

        $this->expectException(TransportException::class);
        $receiver->reject($envelope);
    }
}
