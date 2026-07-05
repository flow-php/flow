<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\EntryTypeResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;

final class EntryTypeResolverTest extends FlowTestCase
{
    public function test_from_definition_keeps_non_nullable_type(): void
    {
        static::assertEquals(type_integer(), (new EntryTypeResolver())->fromDefinition(int_schema('e')));
    }

    public function test_from_definition_keeps_nullable_union_unwrapped(): void
    {
        static::assertEquals(
            type_union(type_integer(), type_string()),
            (new EntryTypeResolver())->fromDefinition(union_schema(
                'e',
                type_union(type_integer(), type_string()),
                true,
            )),
        );
    }

    public function test_from_definition_wraps_nullable_type_as_optional(): void
    {
        static::assertEquals(
            type_optional(type_integer()),
            (new EntryTypeResolver())->fromDefinition(int_schema('e', true)),
        );
    }

    public function test_from_union_falls_back_to_first_castable_member(): void
    {
        static::assertEquals(type_integer(), (new EntryTypeResolver())->fromUnion(
            type_union(type_integer(), type_string()),
            true,
            'e',
        ));
    }

    public function test_from_union_ignores_null_members(): void
    {
        static::assertEquals(type_string(), (new EntryTypeResolver())->fromUnion(
            type_union(type_null(), type_string()),
            'flow',
            'e',
        ));
    }

    public function test_from_union_of_only_null_members_resolves_to_string(): void
    {
        static::assertEquals(type_string(), (new EntryTypeResolver())->fromUnion(
            type_union(type_null(), type_null()),
            null,
            'e',
        ));
    }

    public function test_from_union_resolves_complex_member(): void
    {
        static::assertEquals(
            type_list(type_integer()),
            (new EntryTypeResolver())->fromUnion(type_union(type_list(type_integer()), type_string()), [1, 2, 3], 'e'),
        );
    }

    public function test_from_union_resolves_to_first_valid_member(): void
    {
        $resolver = new EntryTypeResolver();
        $union = type_union(type_integer(), type_string());

        static::assertEquals(type_integer(), $resolver->fromUnion($union, 1, 'e'));
        static::assertEquals(type_string(), $resolver->fromUnion($union, 'flow', 'e'));
        static::assertEquals(type_string(), $resolver->fromUnion($union, '123', 'e'));
    }

    public function test_from_union_skips_members_with_non_casting_exceptions(): void
    {
        static::assertEquals(type_date(), (new EntryTypeResolver())->fromUnion(
            type_union(type_uuid(), type_date()),
            '2024-01-01',
            'e',
        ));
    }

    public function test_from_union_throws_when_value_matches_no_member(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "e": string value does not match any member of union type "date|uuid"');

        (new EntryTypeResolver())->fromUnion(type_union(type_uuid(), type_date()), 'neither uuid nor date', 'e');
    }

    public function test_from_union_unwraps_optional_members(): void
    {
        static::assertEquals(type_string(), (new EntryTypeResolver())->fromUnion(
            type_union(type_optional(type_string()), type_integer()),
            'flow',
            'e',
        ));
    }

    public function test_from_union_with_null_value_resolves_to_first_member(): void
    {
        static::assertEquals(type_integer(), (new EntryTypeResolver())->fromUnion(
            type_union(type_integer(), type_string()),
            null,
            'e',
        ));
    }
}
