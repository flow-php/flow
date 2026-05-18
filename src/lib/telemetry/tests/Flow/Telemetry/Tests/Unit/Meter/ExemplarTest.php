<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter;

use DateTimeImmutable;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Meter\Exemplar;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ExemplarTest extends TestCase
{
    public static function attributeTypeProvider(): Generator
    {
        yield 'string attribute' => [['key' => 'value']];
        yield 'int attribute' => [['count' => 100]];
        yield 'float attribute' => [['ratio' => 0.5]];
        yield 'bool attribute' => [['enabled' => true]];
        yield 'mixed attributes' => [[
            'string' => 'text',
            'int' => 42,
            'float' => 3.14,
            'bool' => false,
        ]];
    }

    public static function valueTypeProvider(): Generator
    {
        yield 'integer' => [42];
        yield 'zero' => [0];
        yield 'negative integer' => [-10];
        yield 'float' => [3.14159];
        yield 'negative float' => [-99.9];
    }

    #[DataProvider('valueTypeProvider')]
    public function test_accepts_int_and_float_values(int|float $value): void
    {
        $exemplar = new Exemplar(
            value: $value,
            timestamp: new DateTimeImmutable(),
            traceId: TraceId::generate(),
            spanId: SpanId::generate(),
        );

        static::assertSame($value, $exemplar->value);
    }

    /**
     * @param array<string, bool|float|int|string> $attributes
     */
    #[DataProvider('attributeTypeProvider')]
    public function test_accepts_various_attribute_types(array $attributes): void
    {
        $exemplar = new Exemplar(
            value: 1,
            timestamp: new DateTimeImmutable(),
            traceId: TraceId::generate(),
            spanId: SpanId::generate(),
            filteredAttributes: $attributes,
        );

        static::assertSame($attributes, $exemplar->filteredAttributes);
    }

    public function test_creates_exemplar_with_all_properties(): void
    {
        $timestamp = new DateTimeImmutable('2024-01-15 10:30:00');
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();

        $exemplar = new Exemplar(
            value: 125.5,
            timestamp: $timestamp,
            traceId: $traceId,
            spanId: $spanId,
            filteredAttributes: ['http.method' => 'GET', 'http.status' => 200],
        );

        static::assertSame(125.5, $exemplar->value);
        static::assertSame($timestamp, $exemplar->timestamp);
        static::assertSame($traceId, $exemplar->traceId);
        static::assertSame($spanId, $exemplar->spanId);
        static::assertSame(['http.method' => 'GET', 'http.status' => 200], $exemplar->filteredAttributes);
    }

    public function test_creates_exemplar_with_empty_attributes(): void
    {
        $exemplar = new Exemplar(
            value: 42,
            timestamp: new DateTimeImmutable(),
            traceId: TraceId::generate(),
            spanId: SpanId::generate(),
        );

        static::assertSame([], $exemplar->filteredAttributes);
    }

    public function test_from_array_creates_exemplar(): void
    {
        $data = [
            'value' => 125.5,
            'timestamp' => '2024-01-15T10:30:00+00:00',
            'traceId' => ['hex' => 'a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8'],
            'spanId' => ['hex' => 'a1b2c3d4e5f6a7b8'],
            'filteredAttributes' => ['http.method' => 'GET'],
        ];

        $exemplar = Exemplar::fromArray($data);

        static::assertSame(125.5, $exemplar->value);
        static::assertSame('a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8', $exemplar->traceId->toHex());
        static::assertSame('a1b2c3d4e5f6a7b8', $exemplar->spanId->toHex());
        static::assertSame(['http.method' => 'GET'], $exemplar->filteredAttributes);
    }

    public function test_normalize_from_array_round_trip(): void
    {
        $timestamp = new DateTimeImmutable('2024-01-15 10:30:00');
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();

        $original = new Exemplar(
            value: 99.9,
            timestamp: $timestamp,
            traceId: $traceId,
            spanId: $spanId,
            filteredAttributes: ['http.status' => 200, 'http.method' => 'POST'],
        );

        $normalized = $original->normalize();
        $restored = Exemplar::fromArray($normalized);

        static::assertSame($original->value, $restored->value);
        static::assertEquals($original->timestamp, $restored->timestamp);
        static::assertSame($original->traceId->toHex(), $restored->traceId->toHex());
        static::assertSame($original->spanId->toHex(), $restored->spanId->toHex());
        static::assertSame($original->filteredAttributes, $restored->filteredAttributes);
    }

    public function test_normalize_returns_array_representation(): void
    {
        $timestamp = new DateTimeImmutable('2024-01-15 10:30:00');
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();

        $exemplar = new Exemplar(
            value: 42,
            timestamp: $timestamp,
            traceId: $traceId,
            spanId: $spanId,
            filteredAttributes: ['key' => 'value'],
        );

        $normalized = $exemplar->normalize();

        static::assertEquals(
            [
                'value' => 42,
                'timestamp' => '2024-01-15T10:30:00+00:00',
                'traceId' => ['hex' => $traceId->toHex()],
                'spanId' => ['hex' => $spanId->toHex()],
                'filteredAttributes' => ['key' => 'value'],
            ],
            $normalized,
        );
    }
}
