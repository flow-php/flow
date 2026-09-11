<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Exception\UnsupportedUnionTypeException;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

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

        static::assertTrue($ref->eval(row(['a' => 1, 'b' => 1]), flow_context()));
    }

    public function test_executing_expression(): void
    {
        $ref = ref('b')->literal(100);

        static::assertSame(100, $ref->eval(row(['a' => 1]), flow_context()));
    }

    public function test_is_even(): void
    {
        $ref = ref('a')->isEven();

        static::assertFalse($ref->eval(row(['a' => 1]), flow_context()));

        static::assertTrue($ref->eval(row(['a' => 2]), flow_context()));
    }

    public function test_is_odd(): void
    {
        $ref = ref('a')->isOdd();

        static::assertTrue($ref->eval(row(['a' => 1]), flow_context()));

        static::assertFalse($ref->eval(row(['a' => 2]), flow_context()));
    }

    public function test_resolving_a_nullable_union_column_names_the_supported_alternatives(): void
    {
        $this->expectException(UnsupportedUnionTypeException::class);
        $this->expectExceptionMessage("str_schema('mixed')");
        $this->expectExceptionMessage("json_schema('mixed')");

        ref('mixed')->resolve(new UnionDefinition('mixed', type_union(type_integer(), type_string()), nullable: true));
    }

    public function test_resolving_a_non_nullable_union_column_is_refused_by_the_same_rule(): void
    {
        $this->expectException(UnsupportedUnionTypeException::class);
        $this->expectExceptionMessage('a column holds exactly one type');

        ref('mixed')->resolve(new UnionDefinition('mixed', type_union(type_integer(), type_string())));
    }

    public function test_resolving_a_null_or_t_union_column_is_that_nullable_column(): void
    {
        static::assertSame(
            '?integer',
            ref('maybe')
                ->resolve(new UnionDefinition('maybe', type_union(type_null(), type_integer())))
                ->returns()
                ->toString(),
        );
    }
}
