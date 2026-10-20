<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter;

use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Tests\Double\NodeNameVisitor;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;

use const NAN;

final class NotTest extends TestCase
{
    public function test_negating_twice_returns_the_child(): void
    {
        $child = parquet_column('id')->lessThan(5);

        static::assertSame($child, $child->negate()->negate());
        static::assertSame($child, (new Not($child))->negate());
    }

    public function test_a_not_visits_as_a_not(): void
    {
        $not = parquet_column('id')->lessThan(5)->negate();

        static::assertSame('not', $not->accept(new NodeNameVisitor()));
        static::assertEquals(new All($not, parquet_column('a')->isNull()), $not->and(parquet_column('a')->isNull()));
        static::assertEquals(new Any($not, parquet_column('a')->isNull()), $not->or(parquet_column('a')->isNull()));
    }

    public function test_a_null_literal_is_refused_in_favour_of_is_null(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A filter literal cannot be null, use isNull()');

        parquet_column('id')->notEquals(null);
    }

    #[TestWith([NAN])]
    public function test_a_nan_literal_is_refused(float $nan): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A filter literal cannot be NaN');

        parquet_column('score')->notIn([1.0, $nan]);
    }
}
