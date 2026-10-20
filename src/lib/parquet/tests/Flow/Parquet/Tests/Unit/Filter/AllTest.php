<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter;

use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Tests\Double\NodeNameVisitor;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;

final class AllTest extends TestCase
{
    public function test_it_visits_as_its_kind(): void
    {
        $composite = new All(
            parquet_column('a')->equals(1),
            parquet_column('b')->isNull(),
            parquet_column('a')->lessThan(3),
        );

        static::assertSame('all', $composite->accept(new NodeNameVisitor()));
        static::assertCount(3, $composite->filters);
    }

    public function test_it_combines_and_negates(): void
    {
        $composite = new All(parquet_column('a')->equals(1));
        $other = parquet_column('other')->isNull();

        static::assertEquals(new All($composite, $other), $composite->and($other));
        static::assertEquals(new Any($composite, $other), $composite->or($other));
        static::assertEquals(new Not($composite), $composite->negate());
    }
}
