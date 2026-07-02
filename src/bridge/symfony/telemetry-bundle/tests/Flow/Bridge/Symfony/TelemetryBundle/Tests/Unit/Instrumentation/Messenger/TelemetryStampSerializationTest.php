<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TelemetryStamp;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\Serializer as MessengerSerializer;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Serializer\Normalizer\ArrayDenormalizer;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;
use Symfony\Component\Serializer\Serializer;

use function class_exists;

#[CoversClass(TelemetryStamp::class)]
final class TelemetryStampSerializationTest extends TestCase
{
    protected function setUp(): void
    {
        if (!class_exists(MessengerSerializer::class) || !class_exists(Serializer::class)) {
            self::markTestSkipped('symfony/messenger or symfony/serializer is not installed');
        }
    }

    public function test_context_survives_php_serializer_round_trip(): void
    {
        $traceparent = '00-abcdef0123456789abcdef0123456789-0123456789abcdef-01';

        $serializer = new PhpSerializer();

        $decoded = $serializer->decode($serializer->encode(new Envelope(new TestMessage('test'), [new TelemetryStamp([
            'traceparent' => $traceparent,
        ])])));

        $stamp = $decoded->last(TelemetryStamp::class);

        static::assertInstanceOf(TelemetryStamp::class, $stamp);
        static::assertSame($traceparent, $stamp->get('traceparent'));
    }

    public function test_context_survives_json_symfony_serializer_round_trip(): void
    {
        $traceparent = '00-abcdef0123456789abcdef0123456789-0123456789abcdef-01';

        $serializer = new MessengerSerializer(
            new Serializer([new ArrayDenormalizer(), new ObjectNormalizer()], [new JsonEncoder()]),
        );

        $decoded = $serializer->decode($serializer->encode(new Envelope(new TestMessage('test'), [new TelemetryStamp([
            'traceparent' => $traceparent,
            'baggage' => 'user.id=42',
        ])])));

        $stamp = $decoded->last(TelemetryStamp::class);

        static::assertInstanceOf(TelemetryStamp::class, $stamp);
        static::assertSame($traceparent, $stamp->get('traceparent'));
        static::assertSame('user.id=42', $stamp->get('baggage'));
    }
}
