<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Flow\Parquet\ThriftModel\ColumnOrder as ThriftColumnOrder;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function var_export;

use const NAN;

final class ColumnOrderTest extends TestCase
{
    /**
     * @return Generator<string, array{FlatColumn, ColumnOrder}>
     */
    public static function writing(): Generator
    {
        yield 'float' => [FlatColumn::float('f'), ColumnOrder::Ieee754TotalOrder];
        yield 'double' => [FlatColumn::double('d'), ColumnOrder::Ieee754TotalOrder];
        yield 'float16' => [FlatColumnMother::float16(), ColumnOrder::Ieee754TotalOrder];
        yield 'int96' => [FlatColumnMother::int96(), ColumnOrder::Int96Timestamp];
        yield 'string' => [FlatColumn::string('s'), ColumnOrder::TypeDefined];
        yield 'int64' => [FlatColumn::int64('i'), ColumnOrder::TypeDefined];
        yield 'decimal' => [FlatColumnMother::flbaDecimal(), ColumnOrder::TypeDefined];
        yield 'boolean' => [FlatColumn::boolean('b'), ColumnOrder::TypeDefined];
    }

    #[DataProvider('writing')]
    public function test_for_writing(FlatColumn $column, ColumnOrder $expected): void
    {
        static::assertSame($expected, ColumnOrder::forWriting($column));
    }

    public function test_from_thrift_without_column_orders_is_absent(): void
    {
        static::assertSame(ColumnOrder::Absent, ColumnOrder::fromThrift(null));
    }

    public function test_from_thrift_without_a_known_member_is_unknown(): void
    {
        static::assertSame(ColumnOrder::Unknown, ColumnOrder::fromThrift(new ThriftColumnOrder()));
    }

    #[TestWith([ColumnOrder::TypeDefined])]
    #[TestWith([ColumnOrder::Ieee754TotalOrder])]
    #[TestWith([ColumnOrder::Int96Timestamp])]
    #[TestWith([ColumnOrder::Unknown])]
    public function test_thrift_round_trip(ColumnOrder $order): void
    {
        static::assertSame($order, ColumnOrder::fromThrift($order->toThrift()));
    }

    public function test_absent_cannot_be_written(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column order Absent cannot be written');

        ColumnOrder::Absent->toThrift();
    }

    /**
     * @return Generator<string, array{FlatColumn, ColumnOrder, bool}>
     */
    public static function trust(): Generator
    {
        yield 'string type defined' => [FlatColumn::string('s'), ColumnOrder::TypeDefined, true];
        yield 'double total order' => [FlatColumn::double('d'), ColumnOrder::Ieee754TotalOrder, true];
        yield 'string absent' => [FlatColumn::string('s'), ColumnOrder::Absent, false];
        yield 'double absent' => [FlatColumn::double('d'), ColumnOrder::Absent, false];
        yield 'flba decimal absent' => [FlatColumnMother::flbaDecimal(), ColumnOrder::Absent, false];
        yield 'int32 absent' => [FlatColumn::int32('i'), ColumnOrder::Absent, false];
        yield 'int64 absent' => [FlatColumn::int64('i'), ColumnOrder::Absent, false];
        yield 'date absent' => [FlatColumn::date('d'), ColumnOrder::Absent, false];
        yield 'timestamp absent' => [FlatColumn::dateTime('t'), ColumnOrder::Absent, false];
        yield 'uint64 absent' => [FlatColumnMother::uint64(), ColumnOrder::Absent, false];
        yield 'uint32 absent' => [FlatColumnMother::uint32(), ColumnOrder::Absent, false];
        yield 'uint64 type defined' => [FlatColumnMother::uint64(), ColumnOrder::TypeDefined, true];
        yield 'string unknown' => [FlatColumn::string('s'), ColumnOrder::Unknown, false];
        yield 'int64 unknown' => [FlatColumn::int64('i'), ColumnOrder::Unknown, false];
        yield 'int96 type defined' => [FlatColumnMother::int96(), ColumnOrder::TypeDefined, false];
        yield 'int96 timestamp order' => [FlatColumnMother::int96(), ColumnOrder::Int96Timestamp, false];
        yield 'interval type defined' => [FlatColumnMother::interval(), ColumnOrder::TypeDefined, false];
    }

    #[DataProvider('trust')]
    public function test_is_trusted(FlatColumn $column, ColumnOrder $order, bool $expected): void
    {
        static::assertSame($expected, $order->isTrusted($column));
    }

    /**
     * @return Generator<string, array{FlatColumn, ColumnOrder, ?float, ?float, ?float, ?float}>
     */
    public static function float_bounds(): Generator
    {
        yield 'type defined NaN bounds ignored' => [
            FlatColumn::double('d'),
            ColumnOrder::TypeDefined,
            NAN,
            NAN,
            null,
            null,
        ];
        yield 'type defined +0.0 min widened' => [
            FlatColumn::double('d'),
            ColumnOrder::TypeDefined,
            0.0,
            1.0,
            -0.0,
            1.0,
        ];
        yield 'type defined -0.0 max widened' => [
            FlatColumn::float('f'),
            ColumnOrder::TypeDefined,
            -1.0,
            -0.0,
            -1.0,
            0.0,
        ];
        yield 'type defined plain bounds kept' => [
            FlatColumn::double('d'),
            ColumnOrder::TypeDefined,
            -2.5,
            3.5,
            -2.5,
            3.5,
        ];
        yield 'total order zeros kept' => [
            FlatColumn::double('d'),
            ColumnOrder::Ieee754TotalOrder,
            0.0,
            -0.0,
            0.0,
            -0.0,
        ];
        yield 'absent untrusted' => [FlatColumn::double('d'), ColumnOrder::Absent, 1.0, 2.0, null, null];
    }

    #[DataProvider('float_bounds')]
    public function test_trusted_float_bounds(
        FlatColumn $column,
        ColumnOrder $order,
        ?float $min,
        ?float $max,
        ?float $trustedMin,
        ?float $trustedMax,
    ): void {
        static::assertSame(var_export($trustedMin, true), var_export($order->trustedMin($column, $min), true));
        static::assertSame(var_export($trustedMax, true), var_export($order->trustedMax($column, $max), true));
    }

    /**
     * @return Generator<string, array{ColumnOrder, string, string, ?string, ?string}>
     */
    public static function float16_bounds(): Generator
    {
        yield 'type defined NaN bounds ignored' => [ColumnOrder::TypeDefined, "\x00\x7e", "\x00\x7e", null, null];
        yield 'type defined +0 min widened' => [
            ColumnOrder::TypeDefined,
            "\x00\x00",
            "\x00\x3c",
            "\x00\x80",
            "\x00\x3c",
        ];
        yield 'type defined -0 max widened' => [
            ColumnOrder::TypeDefined,
            "\x00\xbc",
            "\x00\x80",
            "\x00\xbc",
            "\x00\x00",
        ];
        yield 'total order kept' => [ColumnOrder::Ieee754TotalOrder, "\x00\x00", "\x00\x7e", "\x00\x00", "\x00\x7e"];
    }

    #[DataProvider('float16_bounds')]
    public function test_trusted_float16_bounds(
        ColumnOrder $order,
        string $min,
        string $max,
        ?string $trustedMin,
        ?string $trustedMax,
    ): void {
        static::assertSame($trustedMin, $order->trustedMin(FlatColumnMother::float16(), $min));
        static::assertSame($trustedMax, $order->trustedMax(FlatColumnMother::float16(), $max));
    }

    public function test_trusted_bounds_of_other_types_are_the_bounds(): void
    {
        static::assertSame('a', ColumnOrder::TypeDefined->trustedMin(FlatColumn::string('s'), 'a'));
        static::assertSame(7, ColumnOrder::TypeDefined->trustedMax(FlatColumn::int64('i'), 7));
        static::assertNull(ColumnOrder::TypeDefined->trustedMax(FlatColumn::int64('i'), null));
        static::assertNull(ColumnOrder::Unknown->trustedMin(FlatColumn::int64('i'), 1));
    }
}
