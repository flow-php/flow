<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding\PreImage;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use PHPUnit\Framework\TestCase;

use const PHP_INT_MAX;

final class DoublePreImageTest extends TestCase
{
    public function test_negative_zero_is_normalised_and_equal_to_positive_zero(): void
    {
        $intervals = PhysicalIntervalEdges::intervals(FlatColumn::double('x'), Operator::Equal, -0.0);

        static::assertSame([[0.0, true, 0.0, true]], PhysicalIntervalEdges::of($intervals));
        static::assertTrue($intervals?->covers(PhysicalOrder::Double, -0.0, -0.0));
        static::assertFalse($intervals?->disjoint(PhysicalOrder::Double, -0.0, 0.0));
    }

    public function test_an_int_on_a_double_column_is_exact_up_to_2_pow_53(): void
    {
        static::assertSame(
            [[3.0, true, 3.0, true]],
            PhysicalIntervalEdges::ofComparison(FlatColumn::double('x'), Operator::Equal, 3),
        );
        static::assertNull(PhysicalIntervalEdges::intervals(FlatColumn::double('x'), Operator::Equal, PHP_INT_MAX));
    }
}
