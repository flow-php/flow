<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions\Reference;

use Flow\ETL\Functions\Reference\ResolvedReference;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\NullsOrder;
use Flow\ETL\Values\SortOrder;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\Types\DSL\type_is_nullable;

final class ResolvedReferenceTest extends FlowTestCase
{
    public function test_it_carries_its_type_and_nullability(): void
    {
        $notNull = ref('a')->resolve(int_schema('a'));

        static::assertInstanceOf(ResolvedReference::class, $notNull);
        static::assertSame('integer', $notNull->returns()->toString());
        static::assertFalse(type_is_nullable($notNull->returns()));

        $nullable = ref('a')->resolve(int_schema('a', nullable: true));

        static::assertSame('?integer', $nullable->returns()->toString());
        static::assertTrue(type_is_nullable($nullable->returns()));
    }

    public function test_as_returns_a_copy_carrying_the_type(): void
    {
        $resolved = ref('a')->resolve(int_schema('a'));
        $aliased = $resolved->as('b');

        static::assertNotSame($resolved, $aliased);
        static::assertSame('b', $aliased->name());
        static::assertSame('integer', $aliased->returns()->toString());
    }

    public function test_asc_and_desc_sort_the_column_read_never_the_alias(): void
    {
        $resolved = ref('a')->resolve(int_schema('a'));

        static::assertEquals(
            new SortColumn('a', SortOrder::DESC, NullsOrder::FIRST),
            $resolved->desc(NullsOrder::FIRST),
        );
        static::assertEquals(
            new SortColumn('a', SortOrder::ASC, NullsOrder::LAST),
            $resolved->as('b')->asc(NullsOrder::LAST),
        );
        static::assertEquals(new SortColumn('a', SortOrder::DESC, NullsOrder::LAST), $resolved->desc());
    }
}
