<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Flow\Parquet\Tests\Mother\StatisticsCounterMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Parquet\Binary\decode_f64;
use function pack;
use function str_repeat;
use function var_export;

use const NAN;

final class StatisticsCounterTest extends TestCase
{
    /**
     * @return Generator<string, array{FlatColumn, list<bool|float|int|string>, bool|float|int|string, bool|float|int|string}>
     */
    public static function bounds(): Generator
    {
        yield 'integers' => [FlatColumn::int32('c'), [5, 1, 10, 3], 1, 10];
        yield 'strings' => [FlatColumn::string('c'), ['zebra', 'apple', 'banana'], 'apple', 'zebra'];
        yield 'digit strings in byte order' => [FlatColumn::string('c'), ['9', '10', '100'], '10', '9'];
        yield 'floats' => [FlatColumn::double('c'), [3.14, 1.41, 2.71], 1.41, 3.14];
        yield 'signed zeros' => [FlatColumn::double('c'), [0.0, -0.0], -0.0, 0.0];
        yield 'booleans' => [FlatColumn::boolean('c'), [true, false, true], false, true];
        yield 'single value' => [FlatColumn::int64('c'), [42], 42, 42];
        yield 'descending' => [FlatColumn::int64('c'), [3, 2, 1], 1, 3];
        yield 'uint64 above PHP_INT_MAX' => [FlatColumnMother::uint64(), [5, -1, 7], 5, -1];
        yield 'byte array decimal' => [
            FlatColumnMother::byteArrayDecimal(),
            ["\x12\xD6\x87", "\xED\x29\x79"],
            "\xED\x29\x79",
            "\x12\xD6\x87",
        ];
    }

    /**
     * @param list<bool|float|int|string> $values
     */
    #[DataProvider('bounds')]
    public function test_bounds_follow_the_physical_order(
        FlatColumn $column,
        array $values,
        bool|float|int|string $min,
        bool|float|int|string $max,
    ): void {
        $statistics = StatisticsCounterMother::counted($column, $values);

        static::assertSame(var_export($min, true), var_export($statistics->min(), true));
        static::assertSame(var_export($max, true), var_export($statistics->max(), true));
    }

    public function test_counts_values_and_nulls(): void
    {
        $statistics = StatisticsCounterMother::counted(FlatColumn::int32('c'), [5, null, 10, null, 3]);
        $statistics->addNulls(2);

        static::assertSame(4, $statistics->nullCount());
        static::assertSame(7, $statistics->valuesCount());
        static::assertSame(3, $statistics->notNullCount());
        static::assertSame(3, $statistics->min());
        static::assertSame(10, $statistics->max());
    }

    public function test_negative_null_count_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Null count cannot be negative.');

        StatisticsCounterMother::counted(FlatColumn::int32('c'))->addNulls(-1);
    }

    public function test_an_object_is_not_a_physical_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "c" statistics take physical values, got stdClass');

        StatisticsCounterMother::counted(FlatColumn::string('c'))->addBatch([new stdClass()]);
    }

    public function test_reset_clears_everything(): void
    {
        $statistics = StatisticsCounterMother::counted(FlatColumn::double('c'), [NAN, null, 1.5]);

        $statistics->reset();

        static::assertSame(0, $statistics->valuesCount());
        static::assertSame(0, $statistics->nullCount());
        static::assertNull($statistics->nanCount());
        static::assertNull($statistics->min());
        static::assertNull($statistics->max());
    }

    public function test_merge_combines_counts_and_bounds(): void
    {
        $column = FlatColumn::int64('c');

        $merged = StatisticsCounterMother::counted($column, [
            5,
            null,
        ])->merge(StatisticsCounterMother::counted($column, [1, 9, null]));

        static::assertSame(5, $merged->valuesCount());
        static::assertSame(2, $merged->nullCount());
        static::assertSame(1, $merged->min());
        static::assertSame(9, $merged->max());
    }

    public function test_merge_of_a_part_with_a_lower_min_still_checks_its_max(): void
    {
        $column = FlatColumn::int64('c');

        $merged = StatisticsCounterMother::counted($column, [5, 6])->merge(StatisticsCounterMother::counted($column, [
            1,
            9,
        ]));

        static::assertSame(1, $merged->min());
        static::assertSame(9, $merged->max());
    }

    public function test_merge_with_an_empty_counter_keeps_the_bounds(): void
    {
        $column = FlatColumn::string('c');

        $merged = StatisticsCounterMother::counted($column)->merge(StatisticsCounterMother::counted($column, [
            'b',
            'a',
        ]));

        static::assertSame('a', $merged->min());
        static::assertSame('b', $merged->max());
        static::assertNull(
            StatisticsCounterMother::counted($column)->merge(StatisticsCounterMother::counted($column))->min(),
        );
    }

    public function test_merge_of_different_columns_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot merge statistics for different columns.');

        StatisticsCounterMother::counted(FlatColumn::string(
            'a',
        ))->merge(StatisticsCounterMother::counted(FlatColumn::string('b')));
    }

    public function test_merge_sums_nan_counts_and_replaces_nan_bounds(): void
    {
        $column = FlatColumn::double('d');

        $merged = StatisticsCounterMother::counted($column, [NAN])->merge(StatisticsCounterMother::counted($column, [
            2.5,
            NAN,
        ]));

        static::assertSame(2, $merged->nanCount());
        static::assertSame(2.5, $merged->min());
        static::assertSame(2.5, $merged->max());
    }

    public function test_string_statistics_hold_no_length_prefix_and_no_deprecated_bounds(): void
    {
        $result = StatisticsCounterMother::counted(FlatColumn::string('c'), ['hello', 'world'])->toStatistics();

        static::assertNull($result->min);
        static::assertNull($result->max);
        static::assertSame('hello', $result->minValue);
        static::assertSame('world', $result->maxValue);
        static::assertTrue($result->isMinValueExact);
        static::assertTrue($result->isMaxValueExact);
    }

    public function test_byte_array_decimal_statistics_are_its_unscaled_bytes(): void
    {
        $column = new FlatColumn('c', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::decimal(2, 9));

        $result = StatisticsCounterMother::counted($column, ["\x12\xD6\x87", "\xED\x29\x79"])->toStatistics();

        static::assertSame("\xED\x29\x79", $result->min);
        static::assertSame("\x12\xD6\x87", $result->max);
        static::assertSame("\xED\x29\x79", $result->minValue);
        static::assertSame("\x12\xD6\x87", $result->maxValue);
    }

    public function test_all_null_statistics_hold_no_bounds(): void
    {
        $result = StatisticsCounterMother::counted(FlatColumn::string('c'), [null, null])->toStatistics();

        static::assertNull($result->min);
        static::assertNull($result->maxValue);
        static::assertSame(2, $result->nullCount);
        static::assertFalse($result->isMinValueExact);
        static::assertFalse($result->isMaxValueExact);
        static::assertNull($result->nanCount);
    }

    public function test_nan_is_excluded_from_bounds_and_counted(): void
    {
        $result = StatisticsCounterMother::counted(FlatColumn::double('d'), [
            NAN,
            -0.0,
            NAN,
            0.0,
            null,
        ])->toStatistics();

        static::assertSame(2, $result->nanCount);
        static::assertSame(pack('e', -0.0), $result->minValue);
        static::assertSame(pack('e', 0.0), $result->maxValue);
        static::assertNull($result->min);
        static::assertNull($result->max);
    }

    public function test_only_nan_values_give_nan_bounds(): void
    {
        $result = StatisticsCounterMother::counted(FlatColumn::float('f'), [NAN, NAN])->toStatistics();

        static::assertSame(2, $result->nanCount);
        static::assertSame(pack('g', NAN), $result->minValue);
        static::assertSame(pack('g', NAN), $result->maxValue);
    }

    public function test_float_without_nan_counts_zero_and_all_null_counts_nothing(): void
    {
        static::assertSame(
            0,
            StatisticsCounterMother::counted(FlatColumn::double('d'), [1.5])->toStatistics()->nanCount,
        );
        static::assertNull(StatisticsCounterMother::counted(FlatColumn::double('d'), [null])->toStatistics()->nanCount);
        static::assertNull(StatisticsCounterMother::counted(FlatColumn::int64('i'), [1])->nanCount());
    }

    public function test_page_statistics_of_an_all_null_float_page_count_zero_nans(): void
    {
        static::assertSame(
            0,
            StatisticsCounterMother::counted(FlatColumn::double('d'), [null])->toPageStatistics()->nanCount,
        );
        static::assertSame(
            1,
            StatisticsCounterMother::counted(FlatColumn::double('d'), [NAN])->toPageStatistics()->nanCount,
        );
        static::assertNull(StatisticsCounterMother::counted(
            FlatColumn::int64('i'),
            [null],
        )->toPageStatistics()->nanCount);
    }

    public function test_nan_bounds_follow_the_total_order_of_nans(): void
    {
        $negativeNan = decode_f64(ByteOrder::LITTLE_ENDIAN, "\x00\x00\x00\x00\x00\x00\xf8\xff")[0];

        $rising = StatisticsCounterMother::counted(FlatColumn::double('d'), [$negativeNan, NAN])->toStatistics();
        $falling = StatisticsCounterMother::counted(FlatColumn::double('d'), [NAN, $negativeNan])->toStatistics();

        static::assertSame(pack('e', $negativeNan), $rising->minValue);
        static::assertSame(pack('e', NAN), $rising->maxValue);
        static::assertSame(pack('e', $negativeNan), $falling->minValue);
        static::assertSame(pack('e', NAN), $falling->maxValue);
    }

    public function test_float16_nan_after_a_value_keeps_the_value_bounds(): void
    {
        $statistics = StatisticsCounterMother::counted(FlatColumnMother::float16(), ["\x00\x3c", "\x00\x7e"]);

        static::assertSame(1, $statistics->nanCount());
        static::assertSame("\x00\x3c", $statistics->min());
        static::assertSame("\x00\x3c", $statistics->max());
    }

    public function test_float16_nan_is_counted(): void
    {
        $statistics = StatisticsCounterMother::counted(FlatColumnMother::float16(), ["\x00\x7e", "\x00\x3c"]);

        static::assertSame(1, $statistics->nanCount());
        static::assertSame("\x00\x3c", $statistics->min());
        static::assertSame("\x00\x3c", $statistics->max());
    }

    public function test_deprecated_bounds_are_written_only_for_a_signed_sort_order(): void
    {
        $bool = StatisticsCounterMother::counted(FlatColumn::boolean('b'), [true, false])->toStatistics();

        static::assertSame(
            pack('P', 1),
            StatisticsCounterMother::counted(FlatColumn::int64('i'), [1, 2])->toStatistics()->min,
        );
        static::assertNull(StatisticsCounterMother::counted(FlatColumnMother::uint64(), [1, 2])->toStatistics()->min);
        static::assertSame(
            pack('P', 1),
            StatisticsCounterMother::counted(FlatColumnMother::uint64(), [1, 2])->toStatistics()->minValue,
        );
        static::assertNull($bool->min);
        static::assertSame("\x00", $bool->minValue);
        static::assertSame("\x01", $bool->maxValue);
    }

    public function test_long_string_bounds_are_truncated_and_inexact(): void
    {
        $result = StatisticsCounterMother::counted(FlatColumn::string('s'), [str_repeat('a', 101)])->toStatistics();

        static::assertSame(str_repeat('a', 64), $result->minValue);
        static::assertSame(str_repeat('a', 63) . 'b', $result->maxValue);
        static::assertFalse($result->isMinValueExact);
        static::assertFalse($result->isMaxValueExact);
    }

    public function test_interval_writes_no_bounds(): void
    {
        $result = StatisticsCounterMother::counted(FlatColumnMother::interval(), [str_repeat(
            "\x01",
            12,
        )])->toStatistics();

        static::assertNull($result->minValue);
        static::assertNull($result->maxValue);
        static::assertSame(0, $result->nullCount);
    }
}
