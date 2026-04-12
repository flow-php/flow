<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit\Double;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class FakeSerializer implements SerializerInterface
{
    /**
     * @var list<array<string, mixed>>
     */
    public array $decodeCalls = [];

    /**
     * @var list<Envelope>
     */
    public array $encodeCalls = [];

    /**
     * @param array<string, string> $encodedHeaders
     */
    public function __construct(
        public string $encodedBody = 'encoded-body',
        public array $encodedHeaders = ['type' => 'App\\Message'],
    ) {
    }

    public function decode(array $encodedEnvelope) : Envelope
    {
        $this->decodeCalls[] = $encodedEnvelope;

        return new Envelope((object) [
            'body' => $encodedEnvelope['body'],
            'headers' => $encodedEnvelope['headers'] ?? [],
        ]);
    }

    public function encode(Envelope $envelope) : array
    {
        $this->encodeCalls[] = $envelope;

        return ['body' => $this->encodedBody, 'headers' => $this->encodedHeaders];
    }
}
