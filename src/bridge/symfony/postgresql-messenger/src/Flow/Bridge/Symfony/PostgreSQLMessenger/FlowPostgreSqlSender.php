<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger;

use Flow\Bridge\Symfony\PostgreSQLMessenger\Exception\TransportException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Throwable;

final readonly class FlowPostgreSqlSender implements SenderInterface
{
    public function __construct(
        private Connection $connection,
        private SerializerInterface $serializer,
    ) {}

    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);

        $body = $encodedMessage['body'];
        $headers = $encodedMessage['headers'] ?? [];

        $delay = $envelope->last(DelayStamp::class)?->getDelay() ?? 0;

        try {
            $id = $this->connection->send($body, $headers, $delay);
        } catch (Throwable $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        return $envelope->with(new TransportMessageIdStamp($id));
    }
}
