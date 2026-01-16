<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter;

use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\Meter\Exemplar;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ExemplarTest extends TestCase
{
    public static function attributeTypeProvider() : \Generator
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

    public static function valueTypeProvider() : \Generator
    {
        yield 'integer' => [42];
        yield 'zero' => [0];
        yield 'negative integer' => [-10];
        yield 'float' => [3.14159];
        yield 'negative float' => [-99.9];
    }

    #[DataProvider('valueTypeProvider')]
    public function test_accepts_int_and_float_values(int|float $value) : void
    {
        $exemplar = new Exemplar(
            value: $value,
            timestamp: new \DateTimeImmutable(),
            traceId: TraceId::generate(),
            spanId: SpanId::generate(),
        );

        self::assertSame($value, $exemplar->value);
    }

    /**
     * @param array<string, bool|float|int|string> $attributes
     */
    #[DataProvider('attributeTypeProvider')]
    public function test_accepts_various_attribute_types(array $attributes) : void
    {
        $exemplar = new Exemplar(
            value: 1,
            timestamp: new \DateTimeImmutable(),
            traceId: TraceId::generate(),
            spanId: SpanId::generate(),
            filteredAttributes: $attributes,
        );

        self::assertSame($attributes, $exemplar->filteredAttributes);
    }

    public function test_creates_exemplar_with_all_properties() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();

        $exemplar = new Exemplar(
            value: 125.5,
            timestamp: $timestamp,
            traceId: $traceId,
            spanId: $spanId,
            filteredAttributes: ['http.method' => 'GET', 'http.status' => 200],
        );

        self::assertSame(125.5, $exemplar->value);
        self::assertSame($timestamp, $exemplar->timestamp);
        self::assertSame($traceId, $exemplar->traceId);
        self::assertSame($spanId, $exemplar->spanId);
        self::assertSame(['http.method' => 'GET', 'http.status' => 200], $exemplar->filteredAttributes);
    }

    public function test_creates_exemplar_with_empty_attributes() : void
    {
        $exemplar = new Exemplar(
            value: 42,
            timestamp: new \DateTimeImmutable(),
            traceId: TraceId::generate(),
            spanId: SpanId::generate(),
        );

        self::assertSame([], $exemplar->filteredAttributes);
    }

    public function test_from_array_creates_exemplar() : void
    {
        $data = [
            'value' => 125.5,
            'timestamp' => '2024-01-15T10:30:00+00:00',
            'traceId' => ['hex' => 'a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8'],
            'spanId' => ['hex' => 'a1b2c3d4e5f6a7b8'],
            'filteredAttributes' => ['http.method' => 'GET'],
        ];

        $exemplar = Exemplar::fromArray($data);

        self::assertSame(125.5, $exemplar->value);
        self::assertSame('a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8', $exemplar->traceId->toHex());
        self::assertSame('a1b2c3d4e5f6a7b8', $exemplar->spanId->toHex());
        self::assertSame(['http.method' => 'GET'], $exemplar->filteredAttributes);
    }

    public function test_normalize_from_array_round_trip() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');
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

        self::assertSame($original->value, $restored->value);
        self::assertEquals($original->timestamp, $restored->timestamp);
        self::assertSame($original->traceId->toHex(), $restored->traceId->toHex());
        self::assertSame($original->spanId->toHex(), $restored->spanId->toHex());
        self::assertSame($original->filteredAttributes, $restored->filteredAttributes);
    }

    public function test_normalize_returns_array_representation() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');
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

        self::assertEquals([
            'value' => 42,
            'timestamp' => '2024-01-15T10:30:00+00:00',
            'traceId' => ['hex' => $traceId->toHex()],
            'spanId' => ['hex' => $spanId->toHex()],
            'filteredAttributes' => ['key' => 'value'],
        ], $normalized);
    }
}
