<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding;

use DateTimeImmutable;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Binding\PhysicalInterval;
use Flow\Parquet\Filter\Binding\PhysicalIntervals;
use Flow\Parquet\Filter\Binding\PreImage\IntegerPreImage;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use Flow\Parquet\Tests\Context\PreImageDifferential;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Flow\Parquet\Tests\Mother\OperatorMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\Binary\encode_decimal;

use const INF;
use const NAN;
use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class PhysicalIntervalsTest extends TestCase
{
    public function test_a_set_is_the_union_of_its_equalities_sorted_each_once(): void
    {
        static::assertSame(
            [[1, true, 1, true], [3, true, 3, true]],
            PhysicalIntervalEdges::of(PhysicalIntervals::preImageOfSet(
                new IntegerPreImage(PHP_INT_MIN, PHP_INT_MAX),
                PhysicalOrder::Signed,
                [3, 1, 3],
            )),
        );
        static::assertSame(
            [],
            PhysicalIntervalEdges::of(PhysicalIntervals::preImageOfSet(
                new IntegerPreImage(PHP_INT_MIN, PHP_INT_MAX),
                PhysicalOrder::Signed,
                [],
            )),
        );
        static::assertNull(PhysicalIntervals::preImageOfSet(
            new IntegerPreImage(PHP_INT_MIN, PHP_INT_MAX),
            PhysicalOrder::Signed,
            [1, 'a'],
        ));
        static::assertTrue(
            PhysicalIntervals::preImageOfSet(
                new IntegerPreImage(PHP_INT_MIN, PHP_INT_MAX),
                PhysicalOrder::Signed,
                [],
            )?->isEmpty(),
        );
    }

    #[TestWith([0, true])]
    #[TestWith([1, false])]
    #[TestWith([2, false])]
    #[TestWith([3, true])]
    #[TestWith([5, false])]
    #[TestWith([6, true])]
    #[TestWith([8, true])]
    #[TestWith([9, false])]
    #[TestWith([10, true])]
    #[TestWith([99, true])]
    public function test_contains_finds_the_value_among_sorted_intervals(int $value, bool $contained): void
    {
        $intervals = new PhysicalIntervals(PhysicalOrder::Signed, [
            new PhysicalInterval(null, false, 1, false),
            new PhysicalInterval(3, true, 3, true),
            new PhysicalInterval(5, false, 8, true),
            new PhysicalInterval(10, true, null, false),
        ]);

        static::assertSame($contained, $intervals->contains($value));
    }

    public function test_contains_compares_floats_as_ieee_754(): void
    {
        static::assertFalse((new PhysicalIntervals(PhysicalOrder::Double, [PhysicalInterval::unbounded()]))->contains(
            NAN,
        ));
        static::assertTrue((new PhysicalIntervals(PhysicalOrder::Double, [new PhysicalInterval(
            0.0,
            true,
            0.0,
            true,
        )]))->contains(-0.0));
        static::assertFalse((new PhysicalIntervals(PhysicalOrder::Double, [new PhysicalInterval(
            -0.0,
            false,
            null,
            false,
        )]))->contains(0.0));
        static::assertTrue((new PhysicalIntervals(PhysicalOrder::Double, [new PhysicalInterval(
            -0.0,
            true,
            1.0,
            false,
        )]))->contains(0.0));
    }

    public function test_covers_and_disjoint_honour_each_bound_and_its_inclusion(): void
    {
        $intervals = new PhysicalIntervals(PhysicalOrder::Signed, [
            new PhysicalInterval(1, true, 5, false),
            new PhysicalInterval(8, false, null, false),
        ]);

        static::assertTrue($intervals->covers(1, 4));
        static::assertFalse($intervals->covers(1, 5));
        static::assertFalse($intervals->covers(0, 4));
        static::assertFalse($intervals->covers(8, 9));
        static::assertTrue($intervals->covers(9, PHP_INT_MAX));
        static::assertTrue($intervals->disjoint(5, 8));
        static::assertTrue($intervals->disjoint(-3, 0));
        static::assertFalse($intervals->disjoint(4, 6));
        static::assertFalse($intervals->disjoint(7, 9));
        static::assertTrue((new PhysicalIntervals(PhysicalOrder::Signed, []))->disjoint(1, 1));
        static::assertFalse((new PhysicalIntervals(PhysicalOrder::Signed, []))->covers(1, 1));
        static::assertTrue((new PhysicalIntervals(PhysicalOrder::Signed, [PhysicalInterval::unbounded()]))->covers(
            1,
            1,
        ));
    }

    public function test_covers_and_disjoint_search_among_many_intervals_and_ones_meeting_at_an_exclusive_bound(): void
    {
        $intervals = new PhysicalIntervals(PhysicalOrder::Signed, [
            new PhysicalInterval(0, true, 2, true),
            new PhysicalInterval(10, true, 12, true),
            new PhysicalInterval(20, true, 25, true),
            new PhysicalInterval(25, false, 30, true),
            new PhysicalInterval(40, true, 42, true),
        ]);

        static::assertTrue($intervals->covers(11, 12));
        static::assertTrue($intervals->covers(25, 25));
        static::assertTrue($intervals->covers(26, 30));
        static::assertFalse($intervals->covers(24, 26));
        static::assertFalse($intervals->covers(12, 13));
        static::assertTrue($intervals->contains(25));
        static::assertTrue($intervals->disjoint(31, 39));
        static::assertTrue($intervals->disjoint(-5, -1));
        static::assertTrue($intervals->disjoint(43, 50));
        static::assertFalse($intervals->disjoint(30, 35));
        static::assertFalse($intervals->disjoint(35, 40));
        static::assertFalse($intervals->disjoint(-5, 0));
        static::assertFalse($intervals->disjoint(25, 25));
    }

    public function test_covers_and_disjoint_compare_in_the_column_order(): void
    {
        $intervals = new PhysicalIntervals(PhysicalOrder::Unsigned64, [new PhysicalInterval(null, false, -1, true)]);

        static::assertTrue($intervals->covers(0, PHP_INT_MAX));
        static::assertFalse($intervals->disjoint(PHP_INT_MIN, -2));
    }

    /**
     * @return Generator<string, array{FlatColumn, list<bool|float|int|string>, list<bool|DateTimeImmutable|float|int|string>}>
     */
    public static function columns_physical_values_and_literals(): Generator
    {
        yield 'double' => [
            FlatColumn::double('x'),
            [-INF, -1.0, -0.0, 0.0, 1.0, 2.0 ** 53, INF],
            [INF, -INF, 0.0, -0.0, 1, 1.0, 1.0E39, -1.0E39],
        ];
        yield 'float' => [
            FlatColumn::float('x'),
            [-INF, -3.3999999521443642E+38, -1.0, -0.0, 0.0, 0.10000000149011612, 1.0, 3.3999999521443642E+38, INF],
            [1.0E39, -1.0E39, 0.1, INF, -INF, 1.0E-50, -1.0E-50, 16_777_217],
        ];
        yield 'int64' => [
            FlatColumn::int64('x'),
            [PHP_INT_MIN, -3, -2, 0, 2, 3, PHP_INT_MAX],
            [2.5, -2.5, 2.0, 3, -3, 9_007_199_254_740_992.0],
        ];
        yield 'int32' => [
            FlatColumn::int32('x'),
            [-2_147_483_648, -1, 0, 2, 3, 2_147_483_647],
            [2.5, -0.5, 2_147_483_648, -2_147_483_649, 0],
        ];
        yield 'int8' => [
            new FlatColumn('x', PhysicalType::INT32, logicalType: LogicalType::integer(8, true)),
            [-128, -1, 0, 127],
            [-129, -128, 127, 128, 0, 0.5],
        ];
        yield 'uint8' => [FlatColumnMother::uint8(), [0, 1, 127, 128, 255], [0, 128, 255, 256, -1, 127.5]];
        yield 'uint32' => [
            FlatColumnMother::uint32(),
            [0, 1, 2_147_483_647, -2_147_483_648, -1],
            [0, 1, 2_147_483_648, 4_294_967_295, 4_294_967_296, -1, 2_147_483_647.5],
        ];
        yield 'uint64' => [
            FlatColumnMother::uint64(),
            [0, 1, 9, 10, PHP_INT_MAX, PHP_INT_MIN, -1],
            [0, 1, 9, '10', '9223372036854775808', '18446744073709551615', '18446744073709551616', -1, PHP_INT_MAX],
        ];
        yield 'int64 decimal' => [
            FlatColumnMother::int64Decimal(),
            [-1_000, -105, -11, -10, 0, 10, 11, 105, 1_000],
            [0.1, 0.105, -0.105, 1.0, -2.5, 3.333, 1.0E20, -1.0E20, 0, 1.0E-9, -0.0],
        ];
        yield 'byte array decimal' => [
            new FlatColumn('x', PhysicalType::BYTE_ARRAY, ConvertedType::DECIMAL, LogicalType::decimal(2, 12)),
            [
                encode_decimal(-10.0, 12, 2, null),
                encode_decimal(-0.11, 12, 2, null),
                encode_decimal(0.0, 12, 2, null),
                encode_decimal(0.1, 12, 2, null),
                encode_decimal(0.11, 12, 2, null),
                encode_decimal(10.0, 12, 2, null),
            ],
            [0.1, 0.105, -0.105, -2.5, 3.333, 1.0E20, -1.0E20, 0],
        ];
        yield 'date' => [
            FlatColumn::date('x'),
            [-3, -1, 0, 1, 3],
            [
                new DateTimeImmutable('1970-01-01 00:00 UTC'),
                new DateTimeImmutable('1970-01-02 00:00 +02:00'),
                new DateTimeImmutable('1969-12-31 12:00 UTC'),
                new DateTimeImmutable('1970-01-01 00:00:00.000001 UTC'),
            ],
        ];

        foreach (['MILLIS' => 1, 'MICROS' => 1_000, 'NANOS' => 1_000_000] as $unit => $perMilli) {
            yield 'timestamp ' . $unit => [
                FlatColumnMother::timestamp(TimeUnit::from($unit)),
                [-$perMilli - 1, -$perMilli, -1, 0, 1, $perMilli, $perMilli + 1],
                [
                    new DateTimeImmutable('@0'),
                    new DateTimeImmutable('1970-01-01 00:00:00.0015 UTC'),
                    new DateTimeImmutable('1969-12-31 23:59:59.999 UTC'),
                    new DateTimeImmutable('1970-01-01 00:00:00.001 UTC'),
                ],
            ];
        }

        yield 'boolean' => [FlatColumn::boolean('x'), [false, true], [false, true]];
        yield 'string' => [
            FlatColumn::string('x'),
            ['', 'a', "a\x00", 'ab', "\xC3\xA9", "\xFF"],
            ['a', '', "\xC3\xA9", 'b'],
        ];
    }

    /**
     * @param list<bool|float|int|string> $physicals
     * @param list<bool|DateTimeImmutable|float|int|string> $literals
     */
    #[DataProvider('columns_physical_values_and_literals')]
    public function test_a_physical_value_is_in_the_pre_image_exactly_when_its_decoded_value_matches(
        FlatColumn $column,
        array $physicals,
        array $literals,
    ): void {
        static::assertSame([], PreImageDifferential::mismatches($column, $physicals, $literals));
    }

    /**
     * @param list<array{0: ?float, 1: bool, 2: ?float, 3: bool}> $expected
     */
    #[TestWith(['Equal', INF, [[INF, true, INF, true]]])]
    #[TestWith(['GreaterThanOrEqual', INF, [[INF, true, null, false]]])]
    #[TestWith(['LessThan', INF, [[null, false, INF, false]]])]
    #[TestWith(['GreaterThan', INF, [[INF, false, null, false]]])]
    #[TestWith(['Equal', -INF, [[-INF, true, -INF, true]]])]
    #[TestWith(['GreaterThan', -INF, [[-INF, false, null, false]]])]
    #[TestWith(['LessThanOrEqual', -INF, [[null, false, -INF, true]]])]
    public function test_an_infinity_is_a_value_of_a_double_column(
        string $operator,
        float $literal,
        array $expected,
    ): void {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumn::double('x'),
            OperatorMother::named($operator),
            $literal,
        ));
    }

    /**
     * @param list<array{0: ?int, 1: bool, 2: ?int, 3: bool}> $expected
     */
    #[TestWith(['Equal', 11, []])]
    #[TestWith(['GreaterThanOrEqual', 11, []])]
    #[TestWith(['LessThan', 11, [[null, false, null, false]]])]
    #[TestWith(['LessThan', -1, []])]
    #[TestWith(['GreaterThan', -1, [[null, false, null, false]]])]
    #[TestWith(['LessThanOrEqual', 10, [[null, false, 10, true]]])]
    public function test_an_edge_beyond_the_domain_empties_or_unbounds_the_interval(
        string $operator,
        int $literal,
        array $expected,
    ): void {
        static::assertSame(
            $expected,
            PhysicalIntervalEdges::of(PhysicalIntervals::preImage(
                new IntegerPreImage(0, 10),
                PhysicalOrder::Signed,
                OperatorMother::named($operator),
                $literal,
            )),
        );
    }

    public function test_a_literal_the_pre_image_cannot_compare_has_no_intervals(): void
    {
        static::assertNull(PhysicalIntervals::preImage(
            new IntegerPreImage(0, 10),
            PhysicalOrder::Signed,
            Operator::Equal,
            'a',
        ));
        static::assertNull(PhysicalIntervals::preImageOfSet(
            new IntegerPreImage(0, 10),
            PhysicalOrder::Signed,
            [1, 'a'],
        ));
    }
}
