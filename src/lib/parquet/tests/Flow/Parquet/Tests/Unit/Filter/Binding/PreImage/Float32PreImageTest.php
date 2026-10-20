<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding\PreImage;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use Flow\Parquet\Tests\Mother\OperatorMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use const INF;

final class Float32PreImageTest extends TestCase
{
    /**
     * @param null|list<array{0: ?float, 1: bool, 2: ?float, 3: bool}> $expected
     */
    #[TestWith(['Equal', 0.5, [[0.5, true, 0.5, true]]])]
    #[TestWith(['Equal', 1, [[1.0, true, 1.0, true]]])]
    #[TestWith(['Equal', 0.1, []])]
    #[TestWith(['LessThan', 0.1, [[null, false, 0.10000000149011612, false]]])]
    #[TestWith(['GreaterThan', 0.1, [[0.09999999403953552, false, null, false]]])]
    #[TestWith(['GreaterThan', -0.1, [[-0.10000000149011612, false, null, false]]])]
    #[TestWith(['Equal', 'a', null])]
    public function test_a_float_column_compares_with_the_double_of_each_float32(
        string $operator,
        float|int|string $literal,
        ?array $expected,
    ): void {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumn::float('x'),
            OperatorMother::named($operator),
            $literal,
        ));
    }

    /**
     * @param list<array{0: ?float, 1: bool, 2: ?float, 3: bool}> $expected
     */
    #[TestWith(['Equal', 1.0E39, []])]
    #[TestWith(['GreaterThanOrEqual', 1.0E39, [[INF, true, null, false]]])]
    #[TestWith(['GreaterThan', 1.0E39, [[3.4028234663852886E+38, false, null, false]]])]
    #[TestWith(['LessThan', 1.0E39, [[null, false, INF, false]]])]
    #[TestWith(['LessThanOrEqual', -1.0E39, [[null, false, -INF, true]]])]
    #[TestWith(['GreaterThan', -1.0E39, [[-INF, false, null, false]]])]
    #[TestWith(['Equal', INF, [[INF, true, INF, true]]])]
    public function test_a_literal_beyond_the_largest_float32_lies_next_to_the_infinity(
        string $operator,
        float $literal,
        array $expected,
    ): void {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumn::float('x'),
            OperatorMother::named($operator),
            $literal,
        ));
    }
}
