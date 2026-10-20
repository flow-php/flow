<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function str_repeat;
use function var_export;

use const INF;
use const NAN;
use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class PhysicalOrderTest extends TestCase
{
    /**
     * @return Generator<string, array{FlatColumn, bool|float|int|string, bool|float|int|string, int}>
     */
    public static function comparisons(): Generator
    {
        yield 'boolean false < true' => [FlatColumn::boolean('b'), false, true, -1];
        yield 'boolean equal' => [FlatColumn::boolean('b'), true, true, 0];
        yield 'int32 signed' => [FlatColumn::int32('i'), -1, 1, -1];
        yield 'int64 signed' => [FlatColumn::int64('i'), PHP_INT_MIN, PHP_INT_MAX, -1];
        yield 'date signed' => [FlatColumn::date('d'), -10, 3, -1];
        yield 'uint32 bit pattern above int32 max' => [FlatColumnMother::uint32(), -294967296, 5, 1];
        yield 'uint32 widened value equals its bit pattern' => [FlatColumnMother::uint32(), 4000000000, -294967296, 0];
        yield 'uint64 -1 above PHP_INT_MAX' => [FlatColumnMother::uint64(), -1, PHP_INT_MAX, 1];
        yield 'uint64 PHP_INT_MIN above PHP_INT_MAX' => [FlatColumnMother::uint64(), PHP_INT_MIN, PHP_INT_MAX, 1];
        yield 'uint64 -1 above 5' => [FlatColumnMother::uint64(), -1, 5, 1];
        yield 'uint64 small values' => [FlatColumnMother::uint64(), 3, 5, -1];
        yield 'int64 decimal signed' => [FlatColumnMother::int64Decimal(), -350, 120, -1];
        yield 'double -0.0 < +0.0' => [FlatColumn::double('f'), -0.0, 0.0, -1];
        yield 'double NaN greatest' => [FlatColumn::double('f'), NAN, INF, 1];
        yield 'double -inf smallest' => [FlatColumn::double('f'), -INF, -1.0e308, -1];
        yield 'double negatives' => [FlatColumn::double('f'), -2.5, -1.5, -1];
        yield 'float -0.0 < +0.0' => [FlatColumn::float('f'), -0.0, 0.0, -1];
        yield 'float NaN greatest' => [FlatColumn::float('f'), NAN, INF, 1];
        yield 'float equal' => [FlatColumn::float('f'), 1.5, 1.5, 0];
        yield 'string bytes unsigned' => [FlatColumn::string('s'), '10', '9', -1];
        yield 'string high byte above ascii' => [FlatColumn::string('s'), "\xff", 'a', 1];
        yield 'string prefix shorter' => [FlatColumn::string('s'), 'ab', 'abc', -1];
        yield 'binary equal' => [new FlatColumn('b', PhysicalType::BYTE_ARRAY), "\x00\x01", "\x00\x01", 0];
        yield 'flba bytes unsigned' => [FlatColumn::fixedSizeByteArray('f', 2), "\x80\x00", "\x7f\xff", 1];
        yield 'flba decimal negative < positive' => [FlatColumnMother::flbaDecimal(), "\xfe\xa2", "\x00\x78", -1];
        yield 'flba decimal negatives' => [FlatColumnMother::flbaDecimal(), "\xff\x00", "\xff\x80", -1];
        yield 'byte array decimal sign-extended equal' => [FlatColumnMother::byteArrayDecimal(), "\xff\x80", "\x80", 0];
        yield 'byte array decimal longer positive is greater' => [
            FlatColumnMother::byteArrayDecimal(),
            "\x01\x00",
            "\x7f",
            1,
        ];
        yield 'byte array decimal longer negative is smaller' => [
            FlatColumnMother::byteArrayDecimal(),
            "\xfe\x00",
            "\x80",
            -1,
        ];
        yield 'byte array decimal zero-extended equal' => [FlatColumnMother::byteArrayDecimal(), "\x00\x05", "\x05", 0];
        yield 'byte array decimal empty below value' => [FlatColumnMother::byteArrayDecimal(), '', "\x00", -1];
        yield 'byte array decimal longer than the key width' => [
            FlatColumnMother::byteArrayDecimal(),
            str_repeat("\xff", 40) . "\x80",
            "\x80",
            0,
        ];
        yield 'byte array decimal longer than the key width below' => [
            FlatColumnMother::byteArrayDecimal(),
            str_repeat("\xff", 40) . "\x7f",
            "\xff",
            -1,
        ];
        yield 'float16 -0.0 < +0.0' => [FlatColumnMother::float16(), "\x00\x80", "\x00\x00", -1];
        yield 'float16 negatives' => [FlatColumnMother::float16(), "\x00\xc0", "\x00\xbc", -1];
        yield 'float16 NaN greatest' => [FlatColumnMother::float16(), "\x00\x7e", "\x00\x7c", 1];
    }

    /**
     * @return Generator<string, array{FlatColumn, bool|float|int|string, bool, bool}>
     */
    public static function nan_and_zero(): Generator
    {
        yield 'double NaN' => [FlatColumn::double('d'), NAN, true, false];
        yield 'double +0.0' => [FlatColumn::double('d'), 0.0, false, true];
        yield 'double -0.0' => [FlatColumn::double('d'), -0.0, false, true];
        yield 'double 1.5' => [FlatColumn::double('d'), 1.5, false, false];
        yield 'float NaN' => [FlatColumn::float('f'), NAN, true, false];
        yield 'float16 NaN' => [FlatColumnMother::float16(), "\x00\x7e", true, false];
        yield 'float16 negative NaN' => [FlatColumnMother::float16(), "\x00\xfe", true, false];
        yield 'float16 infinity' => [FlatColumnMother::float16(), "\x00\x7c", false, false];
        yield 'float16 +0' => [FlatColumnMother::float16(), "\x00\x00", false, true];
        yield 'float16 -0' => [FlatColumnMother::float16(), "\x00\x80", false, true];
        yield 'float16 1.0' => [FlatColumnMother::float16(), "\x00\x3c", false, false];
        yield 'int64 zero' => [FlatColumn::int64('i'), 0, false, false];
        yield 'string zero bytes' => [FlatColumn::string('s'), "\x00\x00", false, false];
    }

    /**
     * @return Generator<string, array{FlatColumn, bool}>
     */
    public static function floating_point(): Generator
    {
        yield 'float' => [FlatColumn::float('f'), true];
        yield 'double' => [FlatColumn::double('d'), true];
        yield 'float16' => [FlatColumnMother::float16(), true];
        yield 'int32' => [FlatColumn::int32('i'), false];
        yield 'flba decimal' => [FlatColumnMother::flbaDecimal(), false];
        yield 'string' => [FlatColumn::string('s'), false];
    }

    /**
     * @return Generator<string, array{FlatColumn, bool}>
     */
    public static function signed(): Generator
    {
        yield 'int32' => [FlatColumn::int32('i'), true];
        yield 'int64' => [FlatColumn::int64('i'), true];
        yield 'date' => [FlatColumn::date('d'), true];
        yield 'timestamp' => [FlatColumn::dateTime('t'), true];
        yield 'int64 decimal' => [FlatColumnMother::int64Decimal(), true];
        yield 'flba decimal' => [FlatColumnMother::flbaDecimal(), true];
        yield 'uint32' => [FlatColumnMother::uint32(), false];
        yield 'uint64' => [FlatColumnMother::uint64(), false];
        yield 'boolean' => [FlatColumn::boolean('b'), false];
        yield 'double' => [FlatColumn::double('d'), false];
        yield 'string' => [FlatColumn::string('s'), false];
        yield 'int96' => [FlatColumnMother::int96(), false];
        yield 'uint8' => [FlatColumnMother::uint8(), false];
    }

    /**
     * @return Generator<string, array{FlatColumn}>
     */
    public static function unordered(): Generator
    {
        yield 'int96' => [FlatColumnMother::int96()];
        yield 'interval' => [FlatColumnMother::interval()];
    }

    #[DataProvider('comparisons')]
    public function test_compare(
        FlatColumn $column,
        bool|float|int|string $a,
        bool|float|int|string $b,
        int $expected,
    ): void {
        static::assertSame($expected, PhysicalOrder::forColumn($column)->compare($a, $b));
        static::assertSame(-$expected, PhysicalOrder::forColumn($column)->compare($b, $a));
    }

    #[DataProvider('unordered')]
    public function test_unordered_columns_have_no_order(FlatColumn $column): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('INT96 and INTERVAL have no physical order');

        PhysicalOrder::forColumn($column)->compare("\x00", "\x01");
    }

    #[DataProvider('floating_point')]
    public function test_is_floating_point(FlatColumn $column, bool $expected): void
    {
        static::assertSame($expected, PhysicalOrder::forColumn($column)->isFloatingPoint());
    }

    #[DataProvider('nan_and_zero')]
    public function test_is_nan_and_is_zero(
        FlatColumn $column,
        bool|float|int|string $value,
        bool $nan,
        bool $zero,
    ): void {
        $order = PhysicalOrder::forColumn($column);

        static::assertSame($nan, $order->isNan($value));
        static::assertSame($zero, $order->isZero($value));
    }

    public function test_signed_zeros(): void
    {
        static::assertSame('-0.0', var_export(PhysicalOrder::Double->zero(negative: true), true));
        static::assertSame('0.0', var_export(PhysicalOrder::Float->zero(negative: false), true));
        static::assertSame("\x00\x80", PhysicalOrder::Float16->zero(negative: true));
        static::assertSame("\x00\x00", PhysicalOrder::Float16->zero(negative: false));
    }

    public function test_a_non_floating_point_order_has_no_signed_zero(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Signed has no signed zero');

        PhysicalOrder::Signed->zero(negative: true);
    }

    #[DataProvider('signed')]
    public function test_is_signed(FlatColumn $column, bool $expected): void
    {
        static::assertSame($expected, PhysicalOrder::forColumn($column)->isSigned());
    }
}
