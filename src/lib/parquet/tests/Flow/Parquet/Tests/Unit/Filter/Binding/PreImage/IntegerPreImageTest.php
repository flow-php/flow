<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding\PreImage;

use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\PreImage\IntegerPreImage;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Flow\Parquet\Tests\Mother\OperatorMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use const INF;
use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class IntegerPreImageTest extends TestCase
{
    /**
     * @param null|list<array{0: ?int, 1: bool, 2: ?int, 3: bool}> $expected
     */
    #[TestWith([9_007_199_254_740_992.0, [[9_007_199_254_740_992, true, 9_007_199_254_740_992, true]]])]
    #[TestWith([9_007_199_254_740_994.0, null])]
    #[TestWith([2.5, []])]
    #[TestWith(['1', null])]
    #[TestWith([true, null])]
    public function test_a_float_on_int64_compares_exactly_up_to_2_pow_53(
        bool|float|string $literal,
        ?array $expected,
    ): void {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumn::int64('id'),
            Operator::Equal,
            $literal,
        ));
    }

    /**
     * @param list<array{0: ?int, 1: bool, 2: ?int, 3: bool}> $expected
     */
    #[TestWith(['Equal', 1_099_511_627_776, []])]
    #[TestWith(['GreaterThan', 1_099_511_627_776, []])]
    #[TestWith(['LessThan', 1_099_511_627_776, [[null, false, null, false]]])]
    #[TestWith(['GreaterThan', -1_099_511_627_776, [[null, false, null, false]]])]
    #[TestWith(['LessThanOrEqual', 2_147_483_647, [[null, false, 2_147_483_647, true]]])]
    public function test_an_int32_literal_out_of_range_is_an_empty_or_full_interval(
        string $operator,
        int $literal,
        array $expected,
    ): void {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumn::int32('id'),
            OperatorMother::named($operator),
            $literal,
        ));
    }

    /**
     * @param list<array{0: ?int, 1: bool, 2: ?int, 3: bool}> $expected
     */
    #[TestWith([4_294_967_295, [[4_294_967_295, true, 4_294_967_295, true]]])]
    #[TestWith([4_294_967_296, []])]
    #[TestWith([-1, []])]
    public function test_an_unsigned_int32_literal_is_bound_to_its_decoded_range(int $literal, array $expected): void
    {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumnMother::uint32(),
            Operator::Equal,
            $literal,
        ));
    }

    public function test_an_unsigned_int8_literal_is_bound_to_its_bit_width(): void
    {
        static::assertSame([], PhysicalIntervalEdges::ofComparison(FlatColumnMother::uint8(), Operator::Equal, 256));
        static::assertSame(
            [[255, true, 255, true]],
            PhysicalIntervalEdges::ofComparison(FlatColumnMother::uint8(), Operator::Equal, 255),
        );
    }

    public function test_a_string_literal_on_int64_is_unbound(): void
    {
        static::assertNull(PhysicalIntervalEdges::intervals(FlatColumn::int64('id'), Operator::Equal, '1'));
    }

    /**
     * @param list<array{0: ?int, 1: bool, 2: ?int, 3: bool}> $expected
     */
    #[TestWith(['Equal', 2.5, []])]
    #[TestWith(['GreaterThan', 2.5, [[2, false, null, false]]])]
    #[TestWith(['GreaterThanOrEqual', 2.5, [[3, true, null, false]]])]
    #[TestWith(['LessThan', 2.5, [[null, false, 3, false]]])]
    #[TestWith(['LessThanOrEqual', -2.5, [[null, false, -3, true]]])]
    public function test_a_fraction_lies_between_two_integers(string $operator, float $literal, array $expected): void
    {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumn::int64('id'),
            OperatorMother::named($operator),
            $literal,
        ));
    }

    public function test_an_infinity_is_beyond_exact_integers(): void
    {
        static::assertNull((new IntegerPreImage(PHP_INT_MIN, PHP_INT_MAX))->equal(INF));
        static::assertEquals(new EqualRange(3, 2), (new IntegerPreImage(PHP_INT_MIN, PHP_INT_MAX))->equal(2.5));
    }
}
