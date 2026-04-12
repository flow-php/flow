<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\{KeepaliveReceiverInterface, ListableReceiverInterface, MessageCountAwareInterface};
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

final readonly class FlowPostgreSqlTransport implements KeepaliveReceiverInterface, ListableReceiverInterface, MessageCountAwareInterface, TransportInterface
{
    private FlowPostgreSqlReceiver $receiver;

    private FlowPostgreSqlSender $sender;

    public function __construct(
        Connection $connection,
        SerializerInterface $serializer,
    ) {
        $this->receiver = new FlowPostgreSqlReceiver($connection, $serializer);
        $this->sender = new FlowPostgreSqlSender($connection, $serializer);
    }

    public function ack(Envelope $envelope) : void
    {
        $this->receiver->ack($envelope);
    }

    /**
     * @return iterable<Envelope>
     */
    public function all(?int $limit = null) : iterable
    {
        return $this->receiver->all($limit);
    }

    public function find(mixed $id) : ?Envelope
    {
        return $this->receiver->find($id);
    }

    /**
     * @return iterable<Envelope>
     */
    public function get() : iterable
    {
        return $this->receiver->get();
    }

    public function getMessageCount() : int
    {
        return $this->receiver->getMessageCount();
    }

    public function keepalive(Envelope $envelope, ?int $seconds = null) : void
    {
        $this->receiver->keepalive($envelope, $seconds);
    }

    public function reject(Envelope $envelope) : void
    {
        $this->receiver->reject($envelope);
    }

    public function send(Envelope $envelope) : Envelope
    {
        return $this->sender->send($envelope);
    }
}
