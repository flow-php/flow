<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions\Reference;

use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\NullsOrder;
use Flow\ETL\Values\SortOrder;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

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

    public function test_asc_and_desc_sort_the_column_read_never_the_alias(): void
    {
        static::assertEquals(new SortColumn('a', SortOrder::ASC), ref('a')->asc());
        static::assertEquals(new SortColumn('a', SortOrder::DESC, NullsOrder::LAST), ref('a')->desc());
        static::assertEquals(new SortColumn('a', SortOrder::DESC, NullsOrder::LAST), ref('a')->as('b')->desc());
    }

    public function test_executing_equals_expression(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(ref('b')),
            ['a' => 1, 'b' => 1],
            schema(int_schema('a'), int_schema('b')),
        ));
    }

    public function test_executing_expression(): void
    {
        static::assertSame(100, (new FunctionContext(flow_context()))->eval(
            ref('b')->literal(100),
            ['a' => 1],
            schema(int_schema('a')),
        ));
    }

    public function test_is_even(): void
    {
        $ref = ref('a')->isEven();

        static::assertFalse((new FunctionContext(flow_context()))->eval($ref, ['a' => 1], schema(int_schema('a'))));

        static::assertTrue((new FunctionContext(flow_context()))->eval($ref, ['a' => 2], schema(int_schema('a'))));
    }

    public function test_is_odd(): void
    {
        $ref = ref('a')->isOdd();

        static::assertTrue((new FunctionContext(flow_context()))->eval($ref, ['a' => 1], schema(int_schema('a'))));

        static::assertFalse((new FunctionContext(flow_context()))->eval($ref, ['a' => 2], schema(int_schema('a'))));
    }

    public function test_nulls_default_to_the_smallest_value_and_can_be_moved(): void
    {
        static::assertSame(NullsOrder::FIRST, ref('a')->asc()->nulls);
        static::assertSame(NullsOrder::LAST, ref('a')->desc()->nulls);
        static::assertSame(NullsOrder::LAST, ref('a')->asc(NullsOrder::LAST)->nulls);
        static::assertSame(NullsOrder::FIRST, ref('a')->desc(NullsOrder::FIRST)->nulls);
    }
}
