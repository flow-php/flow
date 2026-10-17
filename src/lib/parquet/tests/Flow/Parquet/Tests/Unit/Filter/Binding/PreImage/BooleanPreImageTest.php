<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding\PreImage;

use Flow\Parquet\Filter\Operator;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use PHPUnit\Framework\TestCase;

final class BooleanPreImageTest extends TestCase
{
    public function test_a_boolean_binds_a_boolean_only(): void
    {
        static::assertSame(
            [[null, false, true, false]],
            PhysicalIntervalEdges::ofComparison(FlatColumn::boolean('b'), Operator::LessThan, true),
        );
        static::assertNull(PhysicalIntervalEdges::intervals(FlatColumn::boolean('b'), Operator::Equal, 1));
    }
}
