<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLMessenger\{Connection, FlowPostgreSqlTransport};
use Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit\Double\{FakeSerializer, SpyClient};
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\{KeepaliveReceiverInterface, ListableReceiverInterface, MessageCountAwareInterface};
use Symfony\Component\Messenger\Transport\{SetupableTransportInterface, TransportInterface};

final class FlowPostgreSqlTransportTest extends TestCase
{
    public function test_ack_delegates_to_receiver() : void
    {
        $client = new SpyClient();
        $client->executeReturn = 1;
        $transport = new FlowPostgreSqlTransport(new Connection($client), new FakeSerializer());

        $envelope = (new Envelope((object) []))->with(new TransportMessageIdStamp('1'));
        $transport->ack($envelope);

        self::assertSame([1], $client->executedQueries[0]['parameters']);
    }

    public function test_all_delegates_to_receiver() : void
    {
        $client = new SpyClient();
        $client->fetchAllReturn = [['id' => 1, 'body' => 'a', 'headers' => '{}']];
        $transport = new FlowPostgreSqlTransport(new Connection($client), new FakeSerializer());

        $envelopes = \iterator_to_array($transport->all(), false);

        self::assertCount(1, $envelopes);
    }

    public function test_does_not_implement_setupable_transport_interface() : void
    {
        self::assertFalse((new \ReflectionClass(FlowPostgreSqlTransport::class))->implementsInterface(SetupableTransportInterface::class));
    }

    public function test_find_delegates_to_receiver() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 5, 'body' => 'a', 'headers' => '{}'];
        $transport = new FlowPostgreSqlTransport(new Connection($client), new FakeSerializer());

        self::assertNotNull($transport->find('5'));
    }

    public function test_get_delegates_to_receiver() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['id' => 1, 'body' => 'a', 'headers' => '{}'];
        $transport = new FlowPostgreSqlTransport(new Connection($client), new FakeSerializer());

        self::assertCount(1, \iterator_to_array($transport->get(), false));
    }

    public function test_get_message_count_delegates_to_receiver() : void
    {
        $client = new SpyClient();
        $client->fetchScalarIntReturn = 5;
        $transport = new FlowPostgreSqlTransport(new Connection($client), new FakeSerializer());

        self::assertSame(5, $transport->getMessageCount());
    }

    public function test_implements_expected_interfaces() : void
    {
        $reflection = new \ReflectionClass(FlowPostgreSqlTransport::class);

        self::assertTrue($reflection->implementsInterface(TransportInterface::class));
        self::assertTrue($reflection->implementsInterface(ListableReceiverInterface::class));
        self::assertTrue($reflection->implementsInterface(MessageCountAwareInterface::class));
        self::assertTrue($reflection->implementsInterface(KeepaliveReceiverInterface::class));
    }

    public function test_keepalive_delegates_to_receiver() : void
    {
        $client = new SpyClient();
        $transport = new FlowPostgreSqlTransport(new Connection($client), new FakeSerializer());

        $envelope = (new Envelope((object) []))->with(new TransportMessageIdStamp('1'));
        $transport->keepalive($envelope);

        self::assertCount(1, $client->executedQueries);
    }

    public function test_reject_delegates_to_receiver() : void
    {
        $client = new SpyClient();
        $client->executeReturn = 1;
        $transport = new FlowPostgreSqlTransport(new Connection($client), new FakeSerializer());

        $envelope = (new Envelope((object) []))->with(new TransportMessageIdStamp('1'));
        $transport->reject($envelope);

        self::assertSame([1], $client->executedQueries[0]['parameters']);
    }

    public function test_send_delegates_to_sender_and_returns_envelope_with_stamp() : void
    {
        $client = new SpyClient();
        $client->fetchOneReturn = ['id' => 99];
        $transport = new FlowPostgreSqlTransport(new Connection($client), new FakeSerializer());

        $result = $transport->send(new Envelope((object) []));

        $stamp = $result->last(TransportMessageIdStamp::class);
        self::assertNotNull($stamp);
        self::assertSame('99', $stamp->getId());
    }
}
