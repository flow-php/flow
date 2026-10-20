<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding\PreImage;

use Flow\Parquet\Filter\Operator;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Flow\Parquet\Tests\Mother\OperatorMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function pack;

final class DecimalPreImageTest extends TestCase
{
    /**
     * @param null|list<array{0: ?int, 1: bool, 2: ?int, 3: bool}> $expected
     */
    #[TestWith(['Equal', 12.34, [[1_234, true, 1_234, true]]])]
    #[TestWith(['Equal', 12, [[1_200, true, 1_200, true]]])]
    #[TestWith(['Equal', 12.345, []])]
    #[TestWith(['LessThan', 12.345, [[null, false, 1_235, false]]])]
    #[TestWith(['GreaterThan', 12.345, [[1_234, false, null, false]]])]
    #[TestWith(['GreaterThan', -12.345, [[-1_235, false, null, false]]])]
    #[TestWith(['Equal', 1.0E20, []])]
    #[TestWith(['LessThan', 1.0E20, [[null, false, null, false]]])]
    #[TestWith(['Equal', 'a', null])]
    public function test_a_decimal_of_precision_10_round_trips_the_literal(
        string $operator,
        float|int|string $literal,
        ?array $expected,
    ): void {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumn::decimal('d', 10, 2),
            OperatorMother::named($operator),
            $literal,
        ));
    }

    public function test_a_decimal_above_precision_15_is_unbound(): void
    {
        static::assertNull(PhysicalIntervalEdges::intervals(FlatColumn::decimal('d', 18, 2), Operator::Equal, 1.5));
    }

    public function test_a_fixed_length_decimal_has_byte_edges(): void
    {
        static::assertSame(
            [[pack('J', 1_234), true, pack('J', 1_234), true]],
            PhysicalIntervalEdges::ofComparison(FlatColumnMother::flbaDecimal(), Operator::Equal, 12.34),
        );
    }
}
