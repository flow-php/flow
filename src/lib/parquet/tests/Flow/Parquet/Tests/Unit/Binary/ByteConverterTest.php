<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Binary;

use Flow\Parquet\Binary\ByteOrder;
use OverflowException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\Binary\decode_decimal;
use function Flow\Parquet\Binary\decode_f32;
use function Flow\Parquet\Binary\decode_f64;
use function Flow\Parquet\Binary\decode_i16;
use function Flow\Parquet\Binary\decode_i32;
use function Flow\Parquet\Binary\decode_i64;
use function Flow\Parquet\Binary\decode_i8;
use function Flow\Parquet\Binary\decode_u16;
use function Flow\Parquet\Binary\decode_u32;
use function Flow\Parquet\Binary\decode_u64;
use function Flow\Parquet\Binary\decode_u8;
use function Flow\Parquet\Binary\encode_decimal;
use function Flow\Parquet\Binary\encode_f32;
use function Flow\Parquet\Binary\encode_f64;
use function Flow\Parquet\Binary\encode_i16;
use function Flow\Parquet\Binary\encode_i32;
use function Flow\Parquet\Binary\encode_i64;
use function Flow\Parquet\Binary\encode_i8;
use function Flow\Parquet\Binary\encode_u16;
use function Flow\Parquet\Binary\encode_u32;
use function Flow\Parquet\Binary\encode_u64;
use function Flow\Parquet\Binary\encode_u8;
use function pack;
use function strlen;
use function unpack;

final class ByteConverterTest extends TestCase
{
    public static function decimalProvider(): array
    {
        return [
            [10.24, 5, 10, 2],
            [0.1, 3, 2, 1],
            [1.234, 4, 4, 3],
            [123.456, 5, 6, 3],
        ];
    }

    public static function int16Provider(): array
    {
        return [
            [0],
            [1],
            [-1],
            [100],
            [-100],
            [32767],
            [-32768],
        ];
    }

    public static function int32Provider(): array
    {
        return [
            [0],
            [1],
            [-1],
            [100],
            [-100],
            [2147483647],
            [-2147483648],
        ];
    }

    public static function int64Provider(): array
    {
        return [
            [0],
            [1],
            [-1],
            [100],
            [-100],
            [2147483647],
            [-2147483648],
            [9223372036854775807],
        ];
    }

    public static function int8Provider(): array
    {
        return [
            [0],
            [1],
            [-1],
            [127],
            [-128],
        ];
    }

    #[DataProvider('decimalProvider')]
    public function test_decimal_roundtrip_big_endian(float $value, int $byteLength, int $precision, int $scale): void
    {
        $encoded = encode_decimal(ByteOrder::BIG_ENDIAN, $value, $byteLength, $precision, $scale);
        $decoded = decode_decimal(ByteOrder::BIG_ENDIAN, $encoded, $precision, $scale);

        static::assertSame($value, $decoded);
    }

    #[DataProvider('decimalProvider')]
    public function test_decimal_roundtrip_little_endian(
        float $value,
        int $byteLength,
        int $precision,
        int $scale,
    ): void {
        $encoded = encode_decimal(ByteOrder::LITTLE_ENDIAN, $value, $byteLength, $precision, $scale);
        $decoded = decode_decimal(ByteOrder::LITTLE_ENDIAN, $encoded, $precision, $scale);

        static::assertSame($value, $decoded);
    }

    public function test_decode_decimal_throws_on_precision_overflow(): void
    {
        $this->expectException(OverflowException::class);
        $this->expectExceptionMessage('exceeds maximum precision of 3 digits');

        $encoded = encode_decimal(ByteOrder::LITTLE_ENDIAN, 12.34, 4, 10, 2);

        decode_decimal(ByteOrder::LITTLE_ENDIAN, $encoded, 3, 2);
    }

    public function test_decode_decimal_validates_precision(): void
    {
        $encoded = encode_decimal(ByteOrder::LITTLE_ENDIAN, 9.99, 4, 10, 2);

        $decoded = decode_decimal(ByteOrder::LITTLE_ENDIAN, $encoded, 3, 2);

        static::assertSame(9.99, $decoded);
    }

    public function test_encode_decimal_throws_on_precision_overflow(): void
    {
        $this->expectException(OverflowException::class);
        $this->expectExceptionMessage('exceeds maximum precision of 10 digits');

        encode_decimal(ByteOrder::LITTLE_ENDIAN, 933162046.43, 5, 10, 2);
    }

    public function test_encode_decode_f32_big_endian(): void
    {
        $value = 3.14;
        $encoded = encode_f32(ByteOrder::BIG_ENDIAN, [$value]);
        $decoded = decode_f32(ByteOrder::BIG_ENDIAN, $encoded)[0];

        static::assertSame(unpack('G', pack('G', $value))[1], $decoded);
    }

    public function test_encode_decode_f32_little_endian(): void
    {
        $value = 3.14;
        $encoded = encode_f32(ByteOrder::LITTLE_ENDIAN, [$value]);
        $decoded = decode_f32(ByteOrder::LITTLE_ENDIAN, $encoded)[0];

        static::assertSame(unpack('g', pack('g', $value))[1], $decoded);
    }

    #[TestWith([0.1])]
    #[TestWith([18.52])]
    #[TestWith([1 / 3])]
    #[TestWith([-0.1])]
    #[TestWith([3.14159265358979])]
    #[TestWith([1.0e-8])]
    #[TestWith([1234567.75])]
    #[TestWith([-2.5e10])]
    public function test_f32_decodes_to_the_exact_widened_binary32(float $value): void
    {
        static::assertSame(
            unpack('g', pack('g', $value))[1],
            decode_f32(ByteOrder::LITTLE_ENDIAN, pack('g', $value))[0],
        );
    }

    public function test_encode_decode_f64_big_endian(): void
    {
        $value = 3.141592653589793;
        $encoded = encode_f64(ByteOrder::BIG_ENDIAN, [$value]);
        $decoded = decode_f64(ByteOrder::BIG_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    public function test_encode_decode_f64_little_endian(): void
    {
        $value = 3.141592653589793;
        $encoded = encode_f64(ByteOrder::LITTLE_ENDIAN, [$value]);
        $decoded = decode_f64(ByteOrder::LITTLE_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    #[DataProvider('int16Provider')]
    public function test_encode_decode_i16_big_endian(int $value): void
    {
        $encoded = encode_i16(ByteOrder::BIG_ENDIAN, [$value]);
        $decoded = decode_i16(ByteOrder::BIG_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    #[DataProvider('int16Provider')]
    public function test_encode_decode_i16_little_endian(int $value): void
    {
        $encoded = encode_i16(ByteOrder::LITTLE_ENDIAN, [$value]);
        $decoded = decode_i16(ByteOrder::LITTLE_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    #[DataProvider('int32Provider')]
    public function test_encode_decode_i32_big_endian(int $value): void
    {
        $encoded = encode_i32(ByteOrder::BIG_ENDIAN, [$value]);
        $decoded = decode_i32(ByteOrder::BIG_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    #[DataProvider('int32Provider')]
    public function test_encode_decode_i32_little_endian(int $value): void
    {
        $encoded = encode_i32(ByteOrder::LITTLE_ENDIAN, [$value]);
        $decoded = decode_i32(ByteOrder::LITTLE_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    #[DataProvider('int64Provider')]
    public function test_encode_decode_i64_big_endian(int $value): void
    {
        $encoded = encode_i64(ByteOrder::BIG_ENDIAN, [$value]);
        $decoded = decode_i64(ByteOrder::BIG_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    #[DataProvider('int64Provider')]
    public function test_encode_decode_i64_little_endian(int $value): void
    {
        $encoded = encode_i64(ByteOrder::LITTLE_ENDIAN, [$value]);
        $decoded = decode_i64(ByteOrder::LITTLE_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    #[DataProvider('int8Provider')]
    public function test_encode_decode_i8(int $value): void
    {
        $encoded = encode_i8([$value]);
        $decoded = decode_i8($encoded)[0];

        static::assertSame($value, $decoded);
    }

    public function test_encode_decode_u16_big_endian(): void
    {
        $value = 65000;
        $encoded = encode_u16(ByteOrder::BIG_ENDIAN, [$value]);
        $decoded = decode_u16(ByteOrder::BIG_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    public function test_encode_decode_u16_little_endian(): void
    {
        $value = 65000;
        $encoded = encode_u16(ByteOrder::LITTLE_ENDIAN, [$value]);
        $decoded = decode_u16(ByteOrder::LITTLE_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    public function test_encode_decode_u32_big_endian(): void
    {
        $value = 4000000000;
        $encoded = encode_u32(ByteOrder::BIG_ENDIAN, [$value]);
        $decoded = decode_u32(ByteOrder::BIG_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    public function test_encode_decode_u32_little_endian(): void
    {
        $value = 4000000000;
        $encoded = encode_u32(ByteOrder::LITTLE_ENDIAN, [$value]);
        $decoded = decode_u32(ByteOrder::LITTLE_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    public function test_encode_decode_u64_big_endian(): void
    {
        $value = 9223372036854775807;
        $encoded = encode_u64(ByteOrder::BIG_ENDIAN, [$value]);
        $decoded = decode_u64(ByteOrder::BIG_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    public function test_encode_decode_u64_little_endian(): void
    {
        $value = 9223372036854775807;
        $encoded = encode_u64(ByteOrder::LITTLE_ENDIAN, [$value]);
        $decoded = decode_u64(ByteOrder::LITTLE_ENDIAN, $encoded)[0];

        static::assertSame($value, $decoded);
    }

    public function test_encode_decode_u8(): void
    {
        $value = 255;
        $encoded = encode_u8([$value]);
        $decoded = decode_u8($encoded)[0];

        static::assertSame($value, $decoded);
    }

    public function test_encode_returns_correct_byte_length_for_f32(): void
    {
        static::assertSame(4, strlen(encode_f32(ByteOrder::LITTLE_ENDIAN, [1.0])));
    }

    public function test_encode_returns_correct_byte_length_for_f64(): void
    {
        static::assertSame(8, strlen(encode_f64(ByteOrder::LITTLE_ENDIAN, [1.0])));
    }

    public function test_encode_returns_correct_byte_length_for_i16(): void
    {
        static::assertSame(2, strlen(encode_i16(ByteOrder::LITTLE_ENDIAN, [1])));
    }

    public function test_encode_returns_correct_byte_length_for_i32(): void
    {
        static::assertSame(4, strlen(encode_i32(ByteOrder::LITTLE_ENDIAN, [1])));
    }

    public function test_encode_returns_correct_byte_length_for_i64(): void
    {
        static::assertSame(8, strlen(encode_i64(ByteOrder::LITTLE_ENDIAN, [1])));
    }

    public function test_encode_returns_correct_byte_length_for_i8(): void
    {
        static::assertSame(1, strlen(encode_i8([1])));
    }

    public function test_encode_returns_correct_byte_length_for_u16(): void
    {
        static::assertSame(2, strlen(encode_u16(ByteOrder::LITTLE_ENDIAN, [1])));
    }

    public function test_encode_returns_correct_byte_length_for_u32(): void
    {
        static::assertSame(4, strlen(encode_u32(ByteOrder::LITTLE_ENDIAN, [1])));
    }

    public function test_encode_returns_correct_byte_length_for_u64(): void
    {
        static::assertSame(8, strlen(encode_u64(ByteOrder::LITTLE_ENDIAN, [1])));
    }

    public function test_encode_returns_correct_byte_length_for_u8(): void
    {
        static::assertSame(1, strlen(encode_u8([1])));
    }
}
