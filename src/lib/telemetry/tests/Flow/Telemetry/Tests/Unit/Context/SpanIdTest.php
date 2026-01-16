<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\SpanId;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class SpanIdTest extends TestCase
{
    public static function provideInvalidBytesLength() : \Generator
    {
        yield 'too short (4 bytes)' => [4];
        yield 'too short (7 bytes)' => [7];
        yield 'too long (9 bytes)' => [9];
        yield 'too long (16 bytes)' => [16];
        yield 'empty' => [0];
    }

    public static function provideInvalidHexStrings() : \Generator
    {
        yield 'too short' => ['00f067aa', 'SpanId hex string must be exactly 16 characters'];
        yield 'too long' => ['00f067aa0ba902b700', 'SpanId hex string must be exactly 16 characters'];
        yield 'non-hex character at end' => ['00f067aa0ba902bg', 'SpanId hex string must contain only hexadecimal characters'];
        yield 'non-hex character at start' => ['z0f067aa0ba902b7', 'SpanId hex string must contain only hexadecimal characters'];
        yield 'spaces' => ['00f067aa 0a902b7', 'SpanId hex string must contain only hexadecimal characters'];
        yield 'empty' => ['', 'SpanId hex string must be exactly 16 characters'];
    }

    public static function provideValidHexStrings() : \Generator
    {
        yield 'lowercase' => ['00f067aa0ba902b7'];
        yield 'uppercase' => ['00F067AA0BA902B7'];
        yield 'mixed case' => ['00f067Aa0Ba902B7'];
        yield 'all f' => ['ffffffffffffffff'];
    }

    public function test_equals_returns_false_for_different_span_ids() : void
    {
        $spanId1 = SpanId::fromHex('00f067aa0ba902b7');
        $spanId2 = SpanId::fromHex('11f067aa0ba902b7');

        self::assertFalse($spanId1->equals($spanId2));
    }

    public function test_equals_returns_true_for_same_span_id() : void
    {
        $hex = '00f067aa0ba902b7';
        $spanId1 = SpanId::fromHex($hex);
        $spanId2 = SpanId::fromHex($hex);

        self::assertTrue($spanId1->equals($spanId2));
    }

    public function test_from_array_creates_span_id() : void
    {
        $hex = '00f067aa0ba902b7';
        $spanId = SpanId::fromArray(['hex' => $hex]);

        self::assertSame($hex, $spanId->toHex());
    }

    public function test_from_bytes_creates_span_id() : void
    {
        $bytes = \random_bytes(8);
        $spanId = SpanId::fromBytes($bytes);

        self::assertSame($bytes, $spanId->toBytes());
    }

    public function test_from_bytes_throws_on_all_zeros() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('SpanId cannot be all zeros');

        SpanId::fromBytes(\str_repeat("\0", 8));
    }

    #[DataProvider('provideInvalidBytesLength')]
    public function test_from_bytes_throws_on_invalid_length(int $length) : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('SpanId must be exactly 8 bytes');

        SpanId::fromBytes(\str_repeat("\x01", $length));
    }

    #[DataProvider('provideValidHexStrings')]
    public function test_from_hex_accepts_valid_hex_strings(string $hex) : void
    {
        $spanId = SpanId::fromHex($hex);

        self::assertSame(\strtolower($hex), $spanId->toHex());
        self::assertSame(8, \strlen($spanId->toBytes()));
    }

    public function test_from_hex_throws_on_all_zeros() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('SpanId cannot be all zeros');

        SpanId::fromHex('0000000000000000');
    }

    #[DataProvider('provideInvalidHexStrings')]
    public function test_from_hex_throws_on_invalid_hex_strings(string $hex, string $expectedMessage) : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage($expectedMessage);

        SpanId::fromHex($hex);
    }

    public function test_generate_creates_span_id() : void
    {
        $spanId = SpanId::generate();

        self::assertSame(16, \strlen($spanId->toHex()));
        self::assertSame(8, \strlen($spanId->toBytes()));
    }

    public function test_generate_creates_unique_span_ids() : void
    {
        $spanId1 = SpanId::generate();
        $spanId2 = SpanId::generate();

        self::assertFalse($spanId1->equals($spanId2));
    }

    public function test_normalize_from_array_round_trip() : void
    {
        $original = SpanId::generate();
        $normalized = $original->normalize();
        $restored = SpanId::fromArray($normalized);

        self::assertTrue($original->equals($restored));
    }

    public function test_normalize_returns_array_with_hex() : void
    {
        $hex = '00f067aa0ba902b7';
        $spanId = SpanId::fromHex($hex);

        self::assertSame(['hex' => $hex], $spanId->normalize());
    }

    public function test_round_trip_hex_to_bytes_to_hex() : void
    {
        $originalHex = '00f067aa0ba902b7';
        $spanId = SpanId::fromHex($originalHex);
        $bytes = $spanId->toBytes();
        $reconstructed = SpanId::fromBytes($bytes);

        self::assertSame($originalHex, $reconstructed->toHex());
    }

    public function test_to_hex_returns_lowercase() : void
    {
        $spanId = SpanId::generate();
        $hex = $spanId->toHex();

        self::assertSame(\strtolower($hex), $hex);
    }

    public function test_to_string_returns_hex() : void
    {
        $hex = '00f067aa0ba902b7';
        $spanId = SpanId::fromHex($hex);

        self::assertSame($hex, (string) $spanId);
    }
}
