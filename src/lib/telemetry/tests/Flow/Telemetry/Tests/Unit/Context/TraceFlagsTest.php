<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\TraceFlags;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TraceFlagsTest extends TestCase
{
    public static function provideInvalidBytes(): \Generator
    {
        yield 'negative' => [-1];
        yield 'too large' => [256];
        yield 'much too large' => [1000];
    }

    public static function provideInvalidHexStrings(): \Generator
    {
        yield 'too short' => ['0', 'TraceFlags hex string must be exactly 2 characters'];
        yield 'too long' => ['001', 'TraceFlags hex string must be exactly 2 characters'];
        yield 'non-hex' => ['0g', 'TraceFlags hex string must contain only hexadecimal characters'];
        yield 'empty' => ['', 'TraceFlags hex string must be exactly 2 characters'];
    }

    public static function provideValidBytes(): \Generator
    {
        yield 'none' => [0x00, false, false];
        yield 'sampled' => [0x01, true, false];
        yield 'random' => [0x02, false, true];
        yield 'sampled and random' => [0x03, true, true];
        yield 'other bits' => [0xFF, true, true];
    }

    public function test_default_creates_unsampled_flags(): void
    {
        $flags = TraceFlags::default();

        static::assertFalse($flags->isSampled());
        static::assertFalse($flags->isRandom());
        static::assertSame(0, $flags->toByte());
    }

    #[DataProvider('provideValidBytes')]
    public function test_from_byte_creates_flags(int $byte, bool $sampled, bool $random): void
    {
        $flags = TraceFlags::fromByte($byte);

        static::assertSame($sampled, $flags->isSampled());
        static::assertSame($random, $flags->isRandom());
    }

    #[DataProvider('provideInvalidBytes')]
    public function test_from_byte_throws_on_invalid_byte(int $byte): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('TraceFlags byte must be between 0 and 255');

        TraceFlags::fromByte($byte);
    }

    public function test_from_hex_creates_sampled_flags(): void
    {
        $flags = TraceFlags::fromHex('01');

        static::assertTrue($flags->isSampled());
        static::assertFalse($flags->isRandom());
    }

    public function test_from_hex_creates_unsampled_flags(): void
    {
        $flags = TraceFlags::fromHex('00');

        static::assertFalse($flags->isSampled());
        static::assertFalse($flags->isRandom());
    }

    #[DataProvider('provideInvalidHexStrings')]
    public function test_from_hex_throws_on_invalid_hex(string $hex, string $expectedMessage): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage($expectedMessage);

        TraceFlags::fromHex($hex);
    }

    public function test_round_trip_hex(): void
    {
        $original = TraceFlags::fromByte(0x03);
        $hex = $original->toHex();
        $restored = TraceFlags::fromHex($hex);

        static::assertSame($original->toByte(), $restored->toByte());
    }

    public function test_sampled_factory_creates_sampled_flags(): void
    {
        $flags = TraceFlags::sampled();

        static::assertTrue($flags->isSampled());
        static::assertFalse($flags->isRandom());
        static::assertSame(0x01, $flags->toByte());
    }

    public function test_to_byte_returns_correct_value(): void
    {
        $flags = TraceFlags::fromByte(0x03);

        static::assertSame(0x03, $flags->toByte());
    }

    public function test_to_hex_returns_lowercase_padded(): void
    {
        $flags = TraceFlags::fromByte(0x01);

        static::assertSame('01', $flags->toHex());
    }

    public function test_to_hex_returns_padded_zero(): void
    {
        $flags = TraceFlags::default();

        static::assertSame('00', $flags->toHex());
    }

    public function test_to_string_returns_hex(): void
    {
        $flags = TraceFlags::sampled();

        static::assertSame('01', (string) $flags);
    }

    public function test_with_random_false_clears_bit(): void
    {
        $flags = TraceFlags::fromByte(0x03);
        $modified = $flags->withRandom(false);

        static::assertTrue($modified->isSampled());
        static::assertFalse($modified->isRandom());
        static::assertSame(0x01, $modified->toByte());
    }

    public function test_with_random_returns_new_instance(): void
    {
        $original = TraceFlags::default();
        $modified = $original->withRandom();

        static::assertNotSame($original, $modified);
        static::assertFalse($original->isRandom());
        static::assertTrue($modified->isRandom());
    }

    public function test_with_sampled_false_clears_bit(): void
    {
        $flags = TraceFlags::fromByte(0x03);
        $modified = $flags->withSampled(false);

        static::assertFalse($modified->isSampled());
        static::assertTrue($modified->isRandom());
        static::assertSame(0x02, $modified->toByte());
    }

    public function test_with_sampled_returns_new_instance(): void
    {
        $original = TraceFlags::default();
        $modified = $original->withSampled();

        static::assertNotSame($original, $modified);
        static::assertFalse($original->isSampled());
        static::assertTrue($modified->isSampled());
    }
}
