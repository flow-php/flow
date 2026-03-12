<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Binary;

use function Flow\Parquet\Binary\{
    decode_decimal,
    decode_f32,
    decode_f64,
    decode_i16,
    decode_i32,
    decode_i64,
    decode_i8,
    decode_u16,
    decode_u32,
    decode_u64,
    decode_u8,
    encode_decimal,
    encode_f32,
    encode_f64,
    encode_i16,
    encode_i32,
    encode_i64,
    encode_i8,
    encode_u16,
    encode_u32,
    encode_u64,
    encode_u8
};
use Flow\Parquet\Binary\ByteOrder;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ByteConverterTest extends TestCase
{
    public static function decimalProvider() : array
    {
        return [
            [10.24, 5, 10, 2],
            [0.1, 3, 2, 1],
            [1.234, 4, 4, 3],
            [123.456, 5, 6, 3],
        ];
    }

    public static function int16Provider() : array
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

    public static function int32Provider() : array
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

    public static function int64Provider() : array
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

    public static function int8Provider() : array
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
    public function test_decimal_roundtrip_big_endian(float $value, int $byteLength, int $precision, int $scale) : void
    {
        $encoded = encode_decimal(ByteOrder::BIG_ENDIAN, $value, $byteLength, $precision, $scale);
        $decoded = decode_decimal(ByteOrder::BIG_ENDIAN, $encoded, $precision, $scale);

        self::assertSame($value, $decoded);
    }

    #[DataProvider('decimalProvider')]
    public function test_decimal_roundtrip_little_endian(float $value, int $byteLength, int $precision, int $scale) : void
    {
        $encoded = encode_decimal(ByteOrder::LITTLE_ENDIAN, $value, $byteLength, $precision, $scale);
        $decoded = decode_decimal(ByteOrder::LITTLE_ENDIAN, $encoded, $precision, $scale);

        self::assertSame($value, $decoded);
    }

    public function test_decode_decimal_throws_on_precision_overflow() : void
    {
        $this->expectException(\OverflowException::class);
        $this->expectExceptionMessage('exceeds maximum precision of 3 digits');

        $encoded = encode_decimal(ByteOrder::LITTLE_ENDIAN, 12.34, 4, 10, 2);

        decode_decimal(ByteOrder::LITTLE_ENDIAN, $encoded, 3, 2);
    }

    public function test_decode_decimal_validates_precision() : void
    {
        $encoded = encode_decimal(ByteOrder::LITTLE_ENDIAN, 9.99, 4, 10, 2);

        $decoded = decode_decimal(ByteOrder::LITTLE_ENDIAN, $encoded, 3, 2);

        self::assertSame(9.99, $decoded);
    }

    public function test_encode_decimal_throws_on_precision_overflow() : void
    {
        $this->expectException(\OverflowException::class);
        $this->expectExceptionMessage('exceeds maximum precision of 10 digits');

        encode_decimal(ByteOrder::LITTLE_ENDIAN, 933162046.43, 5, 10, 2);
    }

    public function test_encode_decode_f32_big_endian() : void
    {
        $value = 3.14;
        $encoded = encode_f32(ByteOrder::BIG_ENDIAN, $value);
        $decoded = decode_f32(ByteOrder::BIG_ENDIAN, $encoded);

        self::assertEqualsWithDelta($value, $decoded, 0.0001);
    }

    public function test_encode_decode_f32_little_endian() : void
    {
        $value = 3.14;
        $encoded = encode_f32(ByteOrder::LITTLE_ENDIAN, $value);
        $decoded = decode_f32(ByteOrder::LITTLE_ENDIAN, $encoded);

        self::assertEqualsWithDelta($value, $decoded, 0.0001);
    }

    public function test_encode_decode_f64_big_endian() : void
    {
        $value = 3.141592653589793;
        $encoded = encode_f64(ByteOrder::BIG_ENDIAN, $value);
        $decoded = decode_f64(ByteOrder::BIG_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    public function test_encode_decode_f64_little_endian() : void
    {
        $value = 3.141592653589793;
        $encoded = encode_f64(ByteOrder::LITTLE_ENDIAN, $value);
        $decoded = decode_f64(ByteOrder::LITTLE_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    #[DataProvider('int16Provider')]
    public function test_encode_decode_i16_big_endian(int $value) : void
    {
        $encoded = encode_i16(ByteOrder::BIG_ENDIAN, $value);
        $decoded = decode_i16(ByteOrder::BIG_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    #[DataProvider('int16Provider')]
    public function test_encode_decode_i16_little_endian(int $value) : void
    {
        $encoded = encode_i16(ByteOrder::LITTLE_ENDIAN, $value);
        $decoded = decode_i16(ByteOrder::LITTLE_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    #[DataProvider('int32Provider')]
    public function test_encode_decode_i32_big_endian(int $value) : void
    {
        $encoded = encode_i32(ByteOrder::BIG_ENDIAN, $value);
        $decoded = decode_i32(ByteOrder::BIG_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    #[DataProvider('int32Provider')]
    public function test_encode_decode_i32_little_endian(int $value) : void
    {
        $encoded = encode_i32(ByteOrder::LITTLE_ENDIAN, $value);
        $decoded = decode_i32(ByteOrder::LITTLE_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    #[DataProvider('int64Provider')]
    public function test_encode_decode_i64_big_endian(int $value) : void
    {
        $encoded = encode_i64(ByteOrder::BIG_ENDIAN, $value);
        $decoded = decode_i64(ByteOrder::BIG_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    #[DataProvider('int64Provider')]
    public function test_encode_decode_i64_little_endian(int $value) : void
    {
        $encoded = encode_i64(ByteOrder::LITTLE_ENDIAN, $value);
        $decoded = decode_i64(ByteOrder::LITTLE_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    #[DataProvider('int8Provider')]
    public function test_encode_decode_i8(int $value) : void
    {
        $encoded = encode_i8($value);
        $decoded = decode_i8($encoded);

        self::assertSame($value, $decoded);
    }

    public function test_encode_decode_u16_big_endian() : void
    {
        $value = 65000;
        $encoded = encode_u16(ByteOrder::BIG_ENDIAN, $value);
        $decoded = decode_u16(ByteOrder::BIG_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    public function test_encode_decode_u16_little_endian() : void
    {
        $value = 65000;
        $encoded = encode_u16(ByteOrder::LITTLE_ENDIAN, $value);
        $decoded = decode_u16(ByteOrder::LITTLE_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    public function test_encode_decode_u32_big_endian() : void
    {
        $value = 4000000000;
        $encoded = encode_u32(ByteOrder::BIG_ENDIAN, $value);
        $decoded = decode_u32(ByteOrder::BIG_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    public function test_encode_decode_u32_little_endian() : void
    {
        $value = 4000000000;
        $encoded = encode_u32(ByteOrder::LITTLE_ENDIAN, $value);
        $decoded = decode_u32(ByteOrder::LITTLE_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    public function test_encode_decode_u64_big_endian() : void
    {
        $value = 9223372036854775807;
        $encoded = encode_u64(ByteOrder::BIG_ENDIAN, $value);
        $decoded = decode_u64(ByteOrder::BIG_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    public function test_encode_decode_u64_little_endian() : void
    {
        $value = 9223372036854775807;
        $encoded = encode_u64(ByteOrder::LITTLE_ENDIAN, $value);
        $decoded = decode_u64(ByteOrder::LITTLE_ENDIAN, $encoded);

        self::assertSame($value, $decoded);
    }

    public function test_encode_decode_u8() : void
    {
        $value = 255;
        $encoded = encode_u8($value);
        $decoded = decode_u8($encoded);

        self::assertSame($value, $decoded);
    }

    public function test_encode_returns_correct_byte_length_for_f32() : void
    {
        self::assertSame(4, \strlen(encode_f32(ByteOrder::LITTLE_ENDIAN, 1.0)));
    }

    public function test_encode_returns_correct_byte_length_for_f64() : void
    {
        self::assertSame(8, \strlen(encode_f64(ByteOrder::LITTLE_ENDIAN, 1.0)));
    }

    public function test_encode_returns_correct_byte_length_for_i16() : void
    {
        self::assertSame(2, \strlen(encode_i16(ByteOrder::LITTLE_ENDIAN, 1)));
    }

    public function test_encode_returns_correct_byte_length_for_i32() : void
    {
        self::assertSame(4, \strlen(encode_i32(ByteOrder::LITTLE_ENDIAN, 1)));
    }

    public function test_encode_returns_correct_byte_length_for_i64() : void
    {
        self::assertSame(8, \strlen(encode_i64(ByteOrder::LITTLE_ENDIAN, 1)));
    }

    public function test_encode_returns_correct_byte_length_for_i8() : void
    {
        self::assertSame(1, \strlen(encode_i8(1)));
    }

    public function test_encode_returns_correct_byte_length_for_u16() : void
    {
        self::assertSame(2, \strlen(encode_u16(ByteOrder::LITTLE_ENDIAN, 1)));
    }

    public function test_encode_returns_correct_byte_length_for_u32() : void
    {
        self::assertSame(4, \strlen(encode_u32(ByteOrder::LITTLE_ENDIAN, 1)));
    }

    public function test_encode_returns_correct_byte_length_for_u64() : void
    {
        self::assertSame(8, \strlen(encode_u64(ByteOrder::LITTLE_ENDIAN, 1)));
    }

    public function test_encode_returns_correct_byte_length_for_u8() : void
    {
        self::assertSame(1, \strlen(encode_u8(1)));
    }
}
