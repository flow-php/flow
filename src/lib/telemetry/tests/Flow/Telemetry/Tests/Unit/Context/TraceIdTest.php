<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\TraceId;
use Generator;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function random_bytes;
use function str_repeat;
use function strlen;
use function strtolower;

final class TraceIdTest extends TestCase
{
    public static function provideInvalidBytesLength(): Generator
    {
        yield 'too short (8 bytes)' => [8];
        yield 'too short (15 bytes)' => [15];
        yield 'too long (17 bytes)' => [17];
        yield 'too long (32 bytes)' => [32];
        yield 'empty' => [0];
    }

    public static function provideInvalidHexStrings(): Generator
    {
        yield 'too short' => ['0af7651916cd43dd', 'TraceId hex string must be exactly 32 characters'];
        yield 'too long' => ['0af7651916cd43dd8448eb211c80319c00', 'TraceId hex string must be exactly 32 characters'];
        yield 'non-hex character at end' => [
            '0af7651916cd43dd8448eb211c80319g',
            'TraceId hex string must contain only hexadecimal characters',
        ];
        yield 'non-hex character at start' => [
            'zaf7651916cd43dd8448eb211c80319c',
            'TraceId hex string must contain only hexadecimal characters',
        ];
        yield 'spaces' => [
            '0af7651916cd43dd 448eb211c80319c',
            'TraceId hex string must contain only hexadecimal characters',
        ];
        yield 'empty' => ['', 'TraceId hex string must be exactly 32 characters'];
    }

    public static function provideValidHexStrings(): Generator
    {
        yield 'lowercase' => ['0af7651916cd43dd8448eb211c80319c'];
        yield 'uppercase' => ['0AF7651916CD43DD8448EB211C80319C'];
        yield 'mixed case' => ['0Af7651916cD43dD8448eB211C80319C'];
        yield 'all f' => ['ffffffffffffffffffffffffffffffff'];
    }

    public function test_equals_returns_false_for_different_trace_ids(): void
    {
        $traceId1 = TraceId::fromHex('0af7651916cd43dd8448eb211c80319c');
        $traceId2 = TraceId::fromHex('1bf7651916cd43dd8448eb211c80319c');

        static::assertFalse($traceId1->equals($traceId2));
    }

    public function test_equals_returns_true_for_same_trace_id(): void
    {
        $hex = '0af7651916cd43dd8448eb211c80319c';
        $traceId1 = TraceId::fromHex($hex);
        $traceId2 = TraceId::fromHex($hex);

        static::assertTrue($traceId1->equals($traceId2));
    }

    public function test_from_array_creates_trace_id(): void
    {
        $hex = '0af7651916cd43dd8448eb211c80319c';
        $traceId = TraceId::fromArray(['hex' => $hex]);

        static::assertSame($hex, $traceId->toHex());
    }

    public function test_from_bytes_allows_all_zeros(): void
    {
        $traceId = TraceId::fromBytes(str_repeat("\0", 16));

        static::assertFalse($traceId->isValid());
        static::assertSame(TraceId::INVALID, $traceId->toHex());
    }

    public function test_from_bytes_creates_trace_id(): void
    {
        $bytes = random_bytes(16);
        $traceId = TraceId::fromBytes($bytes);

        static::assertSame($bytes, $traceId->toBytes());
    }

    #[DataProvider('provideInvalidBytesLength')]
    public function test_from_bytes_throws_on_invalid_length(int $length): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('TraceId must be exactly 16 bytes');

        TraceId::fromBytes(str_repeat("\x01", $length));
    }

    #[DataProvider('provideValidHexStrings')]
    public function test_from_hex_accepts_valid_hex_strings(string $hex): void
    {
        $traceId = TraceId::fromHex($hex);

        static::assertSame(strtolower($hex), $traceId->toHex());
        static::assertSame(16, strlen($traceId->toBytes()));
    }

    public function test_from_hex_allows_all_zeros(): void
    {
        $traceId = TraceId::fromHex('00000000000000000000000000000000');

        static::assertFalse($traceId->isValid());
        static::assertSame(TraceId::INVALID, $traceId->toHex());
    }

    #[DataProvider('provideInvalidHexStrings')]
    public function test_from_hex_throws_on_invalid_hex_strings(string $hex, string $expectedMessage): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($expectedMessage);

        TraceId::fromHex($hex);
    }

    public function test_generate_creates_trace_id(): void
    {
        $traceId = TraceId::generate();

        static::assertSame(32, strlen($traceId->toHex()));
        static::assertSame(16, strlen($traceId->toBytes()));
    }

    public function test_generate_creates_unique_trace_ids(): void
    {
        $traceId1 = TraceId::generate();
        $traceId2 = TraceId::generate();

        static::assertFalse($traceId1->equals($traceId2));
    }

    public function test_invalid_constant_has_correct_value(): void
    {
        static::assertSame('00000000000000000000000000000000', TraceId::INVALID);
        static::assertSame(32, strlen(TraceId::INVALID));
    }

    public function test_invalid_returns_all_zeros_trace_id(): void
    {
        $traceId = TraceId::invalid();

        static::assertSame(TraceId::INVALID, $traceId->toHex());
        static::assertSame(str_repeat("\0", 16), $traceId->toBytes());
    }

    public function test_is_valid_returns_false_for_invalid_trace_id(): void
    {
        $traceId = TraceId::invalid();

        static::assertFalse($traceId->isValid());
    }

    public function test_is_valid_returns_true_for_generated_trace_id(): void
    {
        $traceId = TraceId::generate();

        static::assertTrue($traceId->isValid());
    }

    public function test_normalize_from_array_round_trip(): void
    {
        $original = TraceId::generate();
        $normalized = $original->normalize();
        $restored = TraceId::fromArray($normalized);

        static::assertTrue($original->equals($restored));
    }

    public function test_normalize_returns_array_with_hex(): void
    {
        $hex = '0af7651916cd43dd8448eb211c80319c';
        $traceId = TraceId::fromHex($hex);

        static::assertSame(['hex' => $hex], $traceId->normalize());
    }

    public function test_round_trip_hex_to_bytes_to_hex(): void
    {
        $originalHex = '0af7651916cd43dd8448eb211c80319c';
        $traceId = TraceId::fromHex($originalHex);
        $bytes = $traceId->toBytes();
        $reconstructed = TraceId::fromBytes($bytes);

        static::assertSame($originalHex, $reconstructed->toHex());
    }

    public function test_to_hex_returns_lowercase(): void
    {
        $traceId = TraceId::generate();
        $hex = $traceId->toHex();

        static::assertSame(strtolower($hex), $hex);
    }

    public function test_to_string_returns_hex(): void
    {
        $hex = '0af7651916cd43dd8448eb211c80319c';
        $traceId = TraceId::fromHex($hex);

        static::assertSame($hex, (string) $traceId);
    }
}
