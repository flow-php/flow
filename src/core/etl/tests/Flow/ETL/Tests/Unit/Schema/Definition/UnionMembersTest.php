<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Definition\UnionMembers;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_class_string;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_object;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class UnionMembersTest extends FlowTestCase
{
    public function test_contains_member(): void
    {
        static::assertTrue((new UnionMembers())->contains(
            union_schema('col', type_union(type_integer(), type_string())),
            int_schema('col'),
        ));
    }

    public function test_contains_member_regardless_of_the_nullability_of_the_union(): void
    {
        static::assertTrue((new UnionMembers())->contains(
            union_schema('col', type_union(type_integer(), type_string()), false),
            int_schema('col', true),
        ));
    }

    public function test_contains_rejects_non_member(): void
    {
        static::assertFalse((new UnionMembers())->contains(
            union_schema('col', type_union(type_integer(), type_string())),
            float_schema('col'),
        ));
    }

    public function test_definitions_are_nullable_so_membership_ignores_nullability(): void
    {
        $definitions = (new UnionMembers())->definitions(union_schema(
            'col',
            type_union(type_integer(), type_string()),
            false,
        ));

        static::assertTrue($definitions[0]->isNullable());
        static::assertTrue($definitions[1]->isNullable());
    }

    public function test_definitions_maps_each_member(): void
    {
        $definitions = (new UnionMembers())->definitions(union_schema('col', type_union(
            type_integer(),
            type_string(),
        )));

        static::assertCount(2, $definitions);
        static::assertInstanceOf(IntegerDefinition::class, $definitions[0]);
        static::assertInstanceOf(StringDefinition::class, $definitions[1]);
    }

    public function test_definitions_of_a_union_without_any_mappable_member(): void
    {
        static::assertSame(
            [],
            (new UnionMembers())->definitions(union_schema('col', type_union(type_class_string(), type_object()))),
        );
    }

    public function test_definitions_skip_members_that_cannot_be_mapped(): void
    {
        $definitions = (new UnionMembers())->definitions(union_schema('col', type_union(
            type_class_string(),
            type_string(),
        )));

        static::assertCount(1, $definitions);
        static::assertInstanceOf(StringDefinition::class, $definitions[0]);
    }

    public function test_definitions_unwrap_optional_member_to_its_base(): void
    {
        $definitions = (new UnionMembers())->definitions(union_schema('col', type_union(
            type_optional(type_string()),
            type_integer(),
        )));

        static::assertCount(2, $definitions);
        static::assertInstanceOf(StringDefinition::class, $definitions[0]);
        static::assertInstanceOf(IntegerDefinition::class, $definitions[1]);
    }

    public function test_definitions_use_the_reference_of_the_union(): void
    {
        static::assertSame(
            'col',
            (new UnionMembers())->definitions(union_schema('col', type_union(type_integer(), type_string())))[0]
                ->entry()
                ->name(),
        );
    }

    public function test_unmappable_member_does_not_stop_the_scan(): void
    {
        static::assertTrue((new UnionMembers())->contains(
            union_schema('col', type_union(type_class_string(), type_string())),
            string_schema('col'),
        ));
    }
}
