<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter;

use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Tests\Double\NodeNameVisitor;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;

final class IsNullTest extends TestCase
{
    public function test_it_visits_as_its_kind(): void
    {
        $leaf = parquet_column('id')->isNull();

        static::assertSame('isNull', $leaf->accept(new NodeNameVisitor()));
    }

    public function test_it_combines_and_negates(): void
    {
        $leaf = parquet_column('id')->isNull();
        $other = parquet_column('other')->isNull();

        static::assertEquals(new All($leaf, $other), $leaf->and($other));
        static::assertEquals(new Any($leaf, $other), $leaf->or($other));
        static::assertEquals(new Not($leaf), $leaf->negate());
    }
}
