<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\SortOrder;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class UnresolvedReferenceTest extends FlowTestCase
{
    public function test_as_returns_a_copy_and_does_not_mutate(): void
    {
        $ref = ref('a');
        $aliased = $ref->as('b');

        static::assertNotSame($ref, $aliased);
        static::assertSame('a', $ref->name());
        static::assertFalse($ref->hasAlias());
        static::assertSame('b', $aliased->name());
        static::assertTrue($aliased->hasAlias());
    }

    public function test_asc_and_desc_return_copies_and_do_not_mutate(): void
    {
        $ref = ref('a');
        $desc = $ref->desc();

        static::assertNotSame($ref, $desc);
        static::assertSame(SortOrder::ASC, $ref->sort());
        static::assertSame(SortOrder::DESC, $desc->sort());
        static::assertSame(SortOrder::ASC, $desc->asc()->sort());
    }

    public function test_executing_equals_expression(): void
    {
        $ref = ref('a')->equals(ref('b'));

        static::assertTrue($ref->eval(row(int_entry('a', 1), int_entry('b', 1)), flow_context()));
    }

    public function test_executing_expression(): void
    {
        $ref = ref('b')->literal(100);

        static::assertSame(100, $ref->eval(row(int_entry('a', 1)), flow_context()));
    }

    public function test_is_even(): void
    {
        $ref = ref('a')->isEven();

        static::assertFalse($ref->eval(row(int_entry('a', 1)), flow_context()));

        static::assertTrue($ref->eval(row(int_entry('a', 2)), flow_context()));
    }

    public function test_is_odd(): void
    {
        $ref = ref('a')->isOdd();

        static::assertTrue($ref->eval(row(int_entry('a', 1)), flow_context()));

        static::assertFalse($ref->eval(row(int_entry('a', 2)), flow_context()));
    }
}
