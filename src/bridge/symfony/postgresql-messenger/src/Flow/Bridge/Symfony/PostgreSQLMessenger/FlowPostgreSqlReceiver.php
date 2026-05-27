<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger;

use Flow\Bridge\Symfony\PostgreSQLMessenger\Exception\TransportException;
use JsonException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\KeepaliveReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Throwable;

use function get_debug_type;
use function is_array;
use function is_int;
use function is_string;
use function json_decode;
use function sprintf;

use const JSON_THROW_ON_ERROR;

final readonly class FlowPostgreSqlReceiver implements
    KeepaliveReceiverInterface,
    ListableReceiverInterface,
    MessageCountAwareInterface
{
    public function __construct(
        private Connection $connection,
        private SerializerInterface $serializer,
    ) {}

    public function ack(Envelope $envelope): void
    {
        try {
            $this->connection->ack($this->findTransportMessageId($envelope));
        } catch (Throwable $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    /**
     * @return iterable<Envelope>
     */
    public function all(?int $limit = null): iterable
    {
        try {
            $rows = $this->connection->findAll($limit);
        } catch (Throwable $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        foreach ($rows as $row) {
            yield $this->toEnvelope($row);
        }
    }

    public function find(mixed $id): ?Envelope
    {
        if (!is_int($id) && !is_string($id)) {
            throw TransportException::unexpectedRowShape('id', get_debug_type($id));
        }

        try {
            $row = $this->connection->find($id);
        } catch (Throwable $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        return $row === null ? null : $this->toEnvelope($row);
    }

    /**
     * @return iterable<Envelope>
     */
    public function get(): iterable
    {
        try {
            $row = $this->connection->get();
        } catch (Throwable $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        if ($row === null) {
            return [];
        }

        return [$this->toEnvelope($row)];
    }

    public function getMessageCount(): int
    {
        try {
            return $this->connection->getMessageCount();
        } catch (Throwable $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    public function keepalive(Envelope $envelope, ?int $seconds = null): void
    {
        try {
            $this->connection->keepalive($this->findTransportMessageId($envelope), $seconds);
        } catch (Throwable $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    public function reject(Envelope $envelope): void
    {
        try {
            $this->connection->reject($this->findTransportMessageId($envelope));
        } catch (Throwable $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    private function findTransportMessageId(Envelope $envelope): string
    {
        $stamp = $envelope->last(TransportMessageIdStamp::class);

        if (!$stamp instanceof TransportMessageIdStamp) {
            throw new LogicException(sprintf('No "%s" stamp found on the Envelope.', TransportMessageIdStamp::class));
        }

        /** @var int|string $id */
        $id = $stamp->getId();

        return (string) $id;
    }

    /**
     * @param array<string, mixed> $row
     */
    private function toEnvelope(array $row): Envelope
    {
        $body = is_string($row['body'] ?? null)
            ? $row['body']
            : throw TransportException::unexpectedRowShape('body', get_debug_type($row['body'] ?? null));

        $headersRaw = is_string($row['headers'] ?? null)
            ? $row['headers']
            : throw TransportException::unexpectedRowShape('headers', get_debug_type($row['headers'] ?? null));

        $id = is_int($row['id'] ?? null)
            ? $row['id']
            : (
                is_string($row['id'] ?? null)
                    ? $row['id']
                    : throw TransportException::unexpectedRowShape('id', get_debug_type($row['id'] ?? null))
            );

        try {
            // @mago-expect analysis:mixed-assignment
            $headers = json_decode($headersRaw, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new MessageDecodingFailedException(
                sprintf('Could not decode message headers: %s', $e->getMessage()),
                0,
                $e,
            );
        }

        if (!is_array($headers)) {
            throw new MessageDecodingFailedException('Decoded message headers are not an array.');
        }

        $stringHeaders = [];

        // @mago-expect analysis:mixed-assignment
        foreach ($headers as $key => $value) {
            if (is_string($key) && is_string($value)) {
                $stringHeaders[$key] = $value;
            }
        }

        $envelope = $this->serializer->decode([
            'body' => $body,
            'headers' => $stringHeaders,
        ]);

        return $envelope->with(new TransportMessageIdStamp((string) $id));
    }
}
