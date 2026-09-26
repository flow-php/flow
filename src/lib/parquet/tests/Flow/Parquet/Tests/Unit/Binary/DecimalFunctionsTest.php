<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Binary;

use Flow\Parquet\Exception\InvalidArgumentException;
use Generator;
use OverflowException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function abs;
use function Flow\Parquet\Binary\decimal_unscaled;
use function Flow\Parquet\Binary\decode_decimal;
use function Flow\Parquet\Binary\encode_decimal;
use function ltrim;
use function number_format;
use function strlen;

final class DecimalFunctionsTest extends TestCase
{
    /**
     * @return \Generator<string, array{float, int, int, int}>
     */
    public static function round_trip_provider(): Generator
    {
        $lengths = [1 => [2, 0], 4 => [9, 2], 5 => [11, 2], 8 => [18, 2], 9 => [21, 2], 16 => [38, 2], 21 => [50, 2]];

        foreach ($lengths as $length => [$precision, $scale]) {
            foreach ([0.0, 1.0, -1.0, 12345.67, -12345.67, 1.2345678901234568E+18] as $value) {
                if (strlen(ltrim(number_format(abs($value), $scale, '', ''), '0')) <= $precision) {
                    yield "{$length} bytes, {$value}" => [$value, $precision, $scale, $length];
                }
            }
        }
    }

    #[TestWith(["\x00\x12\xD6\x87", 2, 12345.67])]
    #[TestWith(["\xFF\xED\x29\x79", 2, -12345.67])]
    #[TestWith(["\x12\xD6\x87", 2, 12345.67])]
    #[TestWith(["\xED\x29\x79", 2, -12345.67])]
    #[TestWith(['', 2, 0.0])]
    public function test_decode_decimal(string $bytes, int $scale, float $expected): void
    {
        static::assertSame($expected, decode_decimal($bytes, $scale));
    }

    #[TestWith([12345.67, "\x00\x12\xD6\x87", "\x12\xD6\x87"])]
    #[TestWith([-12345.67, "\xFF\xED\x29\x79", "\xED\x29\x79"])]
    public function test_encode_decimal_bytes(float $value, string $fixedLength, string $minimal): void
    {
        static::assertSame($fixedLength, encode_decimal($value, 9, 2, 4));
        static::assertSame($minimal, encode_decimal($value, 9, 2, null));
    }

    public function test_encode_over_precision_throws(): void
    {
        $this->expectException(OverflowException::class);
        $this->expectExceptionMessage('Decimal value 1234567890 exceeds maximum precision of 9 digits');

        encode_decimal(12345678.9, 9, 2, 4);
    }

    #[TestWith([0.125, '13'])]
    #[TestWith([2.675, '268'])]
    #[TestWith([-0.125, '-13'])]
    #[TestWith([1.005, '101'])]
    public function test_encode_rounds_half_away_from_zero(float $value, string $expected): void
    {
        static::assertSame($expected, decimal_unscaled($value, 9, 2));
    }

    #[TestWith([INF])]
    #[TestWith([-INF])]
    #[TestWith([NAN])]
    public function test_unscaled_rejects_non_finite_values(float $value): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('is not a finite number');

        decimal_unscaled($value, 9, 2);
    }

    #[TestWith([-1234567890123456.8, 18, 2, '-123456789012345680'])]
    #[TestWith([1.2345678901234568E+18, 38, 10, '12345678901234568000000000000'])]
    #[TestWith([1.0E-7, 9, 8, '10'])]
    #[TestWith([1.0E+25, 38, 0, '10000000000000000000000000'])]
    #[TestWith([-0.5, 9, 0, '-1'])]
    public function test_unscaled_is_the_shortest_round_trip_repr(
        float $value,
        int $precision,
        int $scale,
        string $expected,
    ): void {
        static::assertSame($expected, decimal_unscaled($value, $precision, $scale));
    }

    #[TestWith([1.2345678901234568E+39, 50, 10, 21])]
    #[TestWith([-1.2345678901234568E+39, 50, 10, 21])]
    #[TestWith([1.2345678901234568E+18, 38, 10, 12])]
    #[TestWith([-1.2345678901234568E+18, 38, 10, 12])]
    #[TestWith([-128.0, 38, 17, 9])]
    public function test_round_trip_above_int64_with_minimal_length(
        float $value,
        int $precision,
        int $scale,
        int $expectedLength,
    ): void {
        $bytes = encode_decimal($value, $precision, $scale, null);

        static::assertSame($expectedLength, strlen($bytes));
        static::assertSame($value, decode_decimal($bytes, $scale));
    }

    #[DataProvider('round_trip_provider')]
    public function test_round_trip(float $value, int $precision, int $scale, int $length): void
    {
        $bytes = encode_decimal($value, $precision, $scale, $length);

        static::assertSame($length, strlen($bytes));
        static::assertSame($value, decode_decimal($bytes, $scale));
    }
}
