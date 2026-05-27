<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Integration;

use Flow\Bridge\Symfony\PostgreSQLMessenger\Connection;
use Flow\Bridge\Symfony\PostgreSQLMessenger\FlowPostgreSqlTransport;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

use function iterator_to_array;

final class FlowPostgreSqlTransportTest extends MessengerIntegrationTestCase
{
    public function test_find_all_returns_sent_messages(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client),
            new PhpSerializer(),
        );

        $transport->send(new Envelope(new IntegrationTestMessage('one')));
        $transport->send(new Envelope(new IntegrationTestMessage('two')));
        $transport->send(new Envelope(new IntegrationTestMessage('three')));

        static::assertCount(3, iterator_to_array($transport->all(), false));
    }

    public function test_find_by_id_returns_envelope(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client),
            new PhpSerializer(),
        );

        $sent = $transport->send(new Envelope(new IntegrationTestMessage('by-id')));
        $sentStamp = $sent->last(TransportMessageIdStamp::class);
        static::assertNotNull($sentStamp);
        $id = (string) $sentStamp->getId();

        $found = $transport->find($id);

        static::assertNotNull($found);
        $foundStamp = $found->last(TransportMessageIdStamp::class);
        static::assertNotNull($foundStamp);
        static::assertSame($id, $foundStamp->getId());
        $message = $found->getMessage();
        static::assertInstanceOf(IntegrationTestMessage::class, $message);
        static::assertSame('by-id', $message->payload);
    }

    public function test_find_by_id_returns_null_when_not_found(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client),
            new PhpSerializer(),
        );

        static::assertNull($transport->find('999999'));
    }

    public function test_get_message_count_reflects_unhandled_messages(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client),
            new PhpSerializer(),
        );

        static::assertSame(0, $transport->getMessageCount());

        $transport->send(new Envelope(new IntegrationTestMessage('a')));
        $transport->send(new Envelope(new IntegrationTestMessage('b')));

        static::assertSame(2, $transport->getMessageCount());

        iterator_to_array($transport->get(), false);

        static::assertSame(1, $transport->getMessageCount());
    }

    public function test_get_returns_empty_when_no_messages(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client),
            new PhpSerializer(),
        );

        static::assertSame([], iterator_to_array($transport->get(), false));
    }

    public function test_keepalive_keeps_message_marked_as_delivered(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client),
            new PhpSerializer(),
        );

        $transport->send(new Envelope(new IntegrationTestMessage('keep-alive')));
        $received = iterator_to_array($transport->get(), false);
        static::assertCount(1, $received);

        $transport->keepalive($received[0]);

        static::assertSame(0, $transport->getMessageCount());
    }

    public function test_redeliver_timed_out_message(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client, redeliverTimeout: 0),
            new PhpSerializer(),
        );

        $transport->send(new Envelope(new IntegrationTestMessage('timeout')));

        static::assertCount(1, iterator_to_array($transport->get(), false));
        static::assertCount(1, iterator_to_array($transport->get(), false));
    }

    public function test_second_get_does_not_return_already_delivered_message(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client),
            new PhpSerializer(),
        );

        $transport->send(new Envelope(new IntegrationTestMessage('first')));

        iterator_to_array($transport->get(), false);

        static::assertSame([], iterator_to_array($transport->get(), false));
    }

    public function test_send_and_get_message(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client),
            new PhpSerializer(),
        );

        $sent = $transport->send(new Envelope(new IntegrationTestMessage('hello')));
        static::assertNotNull($sent->last(TransportMessageIdStamp::class));

        $received = iterator_to_array($transport->get(), false);

        static::assertCount(1, $received);
        $message = $received[0]->getMessage();
        static::assertInstanceOf(IntegrationTestMessage::class, $message);
        static::assertSame('hello', $message->payload);
    }

    public function test_send_get_and_ack(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client),
            new PhpSerializer(),
        );

        $transport->send(new Envelope(new IntegrationTestMessage('ack-me')));

        $received = iterator_to_array($transport->get(), false);
        static::assertCount(1, $received);

        $transport->ack($received[0]);

        static::assertSame(0, $transport->getMessageCount());
    }

    public function test_send_get_and_reject(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client),
            new PhpSerializer(),
        );

        $transport->send(new Envelope(new IntegrationTestMessage('reject-me')));

        $received = iterator_to_array($transport->get(), false);
        static::assertCount(1, $received);

        $transport->reject($received[0]);

        static::assertSame(0, $transport->getMessageCount());
    }

    public function test_send_with_delay_hides_message_until_available(): void
    {
        $transport = new FlowPostgreSqlTransport(
            new Connection($this->messengerContext()->client),
            new PhpSerializer(),
        );

        $transport->send((new Envelope(new IntegrationTestMessage('delayed')))->with(new DelayStamp(60000)));

        static::assertCount(0, iterator_to_array($transport->get(), false));
    }
}
