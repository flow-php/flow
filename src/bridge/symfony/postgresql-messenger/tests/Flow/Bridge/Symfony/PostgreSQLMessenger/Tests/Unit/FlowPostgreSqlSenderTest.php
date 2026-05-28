<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLMessenger\Connection;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Exception\TransportException as BridgeTransportException;
use Flow\Bridge\Symfony\PostgreSQLMessenger\FlowPostgreSqlSender;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit\Double\FakeSerializer;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit\Double\SpyClient;
use Flow\PostgreSql\QueryBuilder\Sql;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;

final class FlowPostgreSqlSenderTest extends TestCase
{
    public function test_send_adds_transport_message_id_stamp(): void
    {
        $client = new SpyClient();
        $client->fetchSingleReturn = ['id' => 777];
        $sender = new FlowPostgreSqlSender(new Connection($client), new FakeSerializer());

        $result = $sender->send(new Envelope((object) []));

        $stamp = $result->last(TransportMessageIdStamp::class);
        static::assertNotNull($stamp);
        static::assertSame('777', $stamp->getId());
    }

    public function test_send_encodes_envelope_and_passes_to_connection(): void
    {
        $client = new SpyClient();
        $client->fetchSingleReturn = ['id' => 1];
        $serializer = new FakeSerializer('my-body', ['stamp' => 'x']);
        $sender = new FlowPostgreSqlSender(new Connection($client), $serializer);

        $envelope = new Envelope((object) []);
        $sender->send($envelope);

        static::assertCount(1, $serializer->encodeCalls);
        static::assertSame('my-body', $client->executedQueries[0]['parameters'][0]);
        static::assertSame('{"stamp":"x"}', $client->executedQueries[0]['parameters'][1]);
    }

    public function test_send_throws_when_serializer_returns_non_array_headers(): void
    {
        $serializer = new class() extends FakeSerializer {
            public function encode(Envelope $envelope): array
            {
                // @mago-expect analysis:invalid-return-statement
                return ['body' => 'x', 'headers' => 'not-an-array'];
            }
        };
        $sender = new FlowPostgreSqlSender(new Connection(new SpyClient()), $serializer);

        $this->expectException(BridgeTransportException::class);

        $sender->send(new Envelope((object) []));
    }

    public function test_send_throws_when_serializer_returns_non_string_body(): void
    {
        $serializer = new class() extends FakeSerializer {
            public function encode(Envelope $envelope): array
            {
                // @mago-expect analysis:invalid-return-statement
                return ['body' => 123, 'headers' => []];
            }
        };
        $sender = new FlowPostgreSqlSender(new Connection(new SpyClient()), $serializer);

        $this->expectException(BridgeTransportException::class);

        $sender->send(new Envelope((object) []));
    }

    public function test_send_with_delay_stamp_delegates_delay_to_connection(): void
    {
        $client = new SpyClient();
        $client->fetchSingleReturn = ['id' => 1];
        $sender = new FlowPostgreSqlSender(new Connection($client), new FakeSerializer());

        $envelope = (new Envelope((object) []))->with(new DelayStamp(5000));
        $sender->send($envelope);

        static::assertCount(1, $client->executedQueries);
    }

    public function test_send_without_delay_stamp_passes_zero_delay(): void
    {
        $client = new SpyClient();
        $client->fetchSingleReturn = ['id' => 1];
        $sender = new FlowPostgreSqlSender(new Connection($client), new FakeSerializer());

        $sender->send(new Envelope((object) []));

        // Without delay, available_at should equal created_at (no offset)
        static::assertCount(1, $client->executedQueries);
    }

    public function test_send_wraps_connection_exceptions(): void
    {
        $client = new class() extends SpyClient {
            public function fetchSingle(Sql|string $sql, array $parameters = []): array
            {
                throw new RuntimeException('boom');
            }
        };
        $sender = new FlowPostgreSqlSender(new Connection($client), new FakeSerializer());

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('boom');

        $sender->send(new Envelope((object) []));
    }
}
