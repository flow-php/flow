<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding\PreImage;

use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Flow\Parquet\Tests\Mother\OperatorMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use const PHP_INT_MIN;

final class UnsignedInt64PreImageTest extends TestCase
{
    /**
     * @param null|list<array{0: ?int, 1: bool, 2: ?int, 3: bool}> $expected
     */
    #[TestWith(['Equal', '18446744073709551615', [[-1, true, -1, true]]])]
    #[TestWith(['Equal', 5, [[5, true, 5, true]]])]
    #[TestWith(['LessThan', '9223372036854775808', [[null, false, PHP_INT_MIN, false]]])]
    #[TestWith(['Equal', -1, []])]
    #[TestWith(['GreaterThan', -1, [[null, false, null, false]]])]
    #[TestWith(['LessThan', -1, []])]
    #[TestWith(['Equal', '18446744073709551616', []])]
    #[TestWith(['GreaterThan', '18446744073709551616', []])]
    #[TestWith(['LessThan', '18446744073709551616', [[null, false, null, false]]])]
    #[TestWith(['Equal', '007', null])]
    #[TestWith(['Equal', '1.0', null])]
    #[TestWith(['Equal', 1.0, [[1, true, 1, true]]])]
    #[TestWith(['Equal', 2.5, []])]
    #[TestWith(['GreaterThan', 2.5, [[2, false, null, false]]])]
    #[TestWith(['LessThan', -0.5, [[null, false, 0, false]]])]
    #[TestWith(['GreaterThan', -0.5, [[null, false, null, false]]])]
    #[TestWith(['Equal', 1.0e20, null])]
    public function test_uint64_binds_to_the_int64_bit_pattern(
        string $operator,
        float|int|string $literal,
        ?array $expected,
    ): void {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumnMother::uint64(),
            OperatorMother::named($operator),
            $literal,
        ));
    }
}
