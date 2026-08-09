<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\definition_from_array;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_class_string;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;

final class UnionDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            union_schema('col', type_union(type_string(), type_integer())),
            union_schema('col', type_union(type_string(), type_integer())),
            true,
        ];

        yield 'same type different name' => [
            union_schema('col', type_union(type_string(), type_integer())),
            union_schema('other', type_union(type_string(), type_integer())),
            false,
        ];

        yield 'not nullable with nullable' => [
            union_schema('col', type_union(type_string(), type_integer()), false),
            union_schema('col', type_union(type_string(), type_integer()), true),
            false,
        ];

        yield 'nullable with not nullable' => [
            union_schema('col', type_union(type_string(), type_integer()), true),
            union_schema('col', type_union(type_string(), type_integer()), false),
            true,
        ];

        yield 'different member types' => [
            union_schema('col', type_union(type_string(), type_integer())),
            union_schema('col', type_union(type_string(), type_boolean())),
            false,
        ];

        yield 'union with its string member' => [
            union_schema('col', type_union(type_string(), type_integer())),
            string_schema('col'),
            true,
        ];

        yield 'union with its integer member' => [
            union_schema('col', type_union(type_string(), type_integer())),
            int_schema('col'),
            true,
        ];

        yield 'union with a type that is not a member' => [
            union_schema('col', type_union(type_string(), type_integer())),
            float_schema('col'),
            false,
        ];

        yield 'nullable union with nullable member' => [
            union_schema('col', type_union(type_string(), type_integer()), true),
            int_schema('col', true),
            true,
        ];

        yield 'not nullable union with nullable member' => [
            union_schema('col', type_union(type_string(), type_integer()), false),
            int_schema('col', true),
            false,
        ];

        yield 'union with optional member' => [
            union_schema('col', type_union(type_optional(type_string()), type_integer())),
            string_schema('col'),
            true,
        ];

        yield 'union with a member that has no definition' => [
            union_schema('col', type_union(type_class_string(), type_string())),
            string_schema('col'),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same union' => [
            union_schema('col', type_union(type_string(), type_integer())),
            union_schema('col', type_union(type_string(), type_integer())),
            union_schema('col', type_union(type_string(), type_integer())),
        ];

        yield 'makes nullable when other is nullable' => [
            union_schema('col', type_union(type_string(), type_integer()), false),
            union_schema('col', type_union(type_string(), type_integer()), true),
            union_schema('col', type_union(type_string(), type_integer()), true),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_array_member_is_normalized_to_json(): void
    {
        static::assertSame(
            'json|string',
            union_schema('col', type_union(type_string(), type_array()))->type()->toString(),
        );
    }

    public function test_normalized_array_member_survives_a_normalize_round_trip(): void
    {
        $def = union_schema('col', type_union(type_string(), type_array()));

        static::assertEquals($def, definition_from_array($def->normalize()));
    }

    public function test_definition_from_type_creates_union_definition(): void
    {
        $definition = definition_from_type('col', type_union(type_string(), type_integer()));

        static::assertInstanceOf(UnionDefinition::class, $definition);
        static::assertSame('col', $definition->entry()->name());
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        static::assertFalse($def->matches(int_entry('col', null)));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        static::assertFalse($def->matches(str_entry('other', 'value')));
    }

    public function test_does_not_match_entry_with_value_outside_of_union(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        static::assertFalse($def->matches(bool_entry('col', true)));
    }

    public function test_entry_class_taken_from_left_union_member(): void
    {
        static::assertSame(
            StringEntry::class,
            union_schema('col', type_union(type_string(), type_integer()))->entryClass(),
        );
        static::assertSame(
            IntegerEntry::class,
            union_schema('col', type_union(type_integer(), type_string()))->entryClass(),
        );
    }

    public function test_entry_class_with_null_left_union_member(): void
    {
        static::assertSame(NullEntry::class, union_schema('col', type_union(type_null(), type_string()))->entryClass());
    }

    public function test_entry_class_with_optional_left_union_member(): void
    {
        static::assertSame(
            StringEntry::class,
            union_schema('col', type_union(type_optional(type_string()), type_integer()))->entryClass(),
        );
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     */
    #[DataProvider('provideIsCompatibleCases')]
    public function test_is_compatible(Definition $definition, Definition $other, bool $expected): void
    {
        static::assertSame($expected, $definition->isCompatible($other));
    }

    public function test_is_compatible_with_list_of_union_elements(): void
    {
        static::assertTrue(definition_from_type(
            'col',
            type_list(type_union(type_integer(), type_string())),
        )->isCompatible(definition_from_type('col', type_list(type_integer()))));
    }

    public function test_is_same_with_different_metadata(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), false, Metadata::with('key', 'value1'));
        $other = union_schema('col', type_union(type_string(), type_integer()), false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), true);
        $other = union_schema('col', type_union(type_string(), type_integer()), false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));
        $other = string_schema('col');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), true, Metadata::with('key', 'value'));
        $other = union_schema('col', type_union(type_string(), type_integer()), true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_value_valid_for_any_member(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        static::assertTrue($def->matches(str_entry('col', 'value')));
        static::assertTrue($def->matches(int_entry('col', 1)));
    }

    public function test_member_for_carries_nullability_and_metadata(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), true, Metadata::with('key', 'value'));

        static::assertEquals(int_schema('col', true, Metadata::with('key', 'value')), $def->memberFor(1));
    }

    public function test_member_for_resolves_the_first_non_null_member_for_null(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        static::assertEquals(string_schema('col'), $def->memberFor(null));
    }

    public function test_member_for_resolves_the_member_accepting_the_value(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        static::assertEquals(int_schema('col'), $def->memberFor(1));
        static::assertEquals(string_schema('col'), $def->memberFor('value'));
    }

    public function test_member_for_throws_for_a_value_outside_every_member(): void
    {
        $def = union_schema('col', type_union(type_uuid(), type_datetime()));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "col": array value does not match any member of union type');

        $def->memberFor([1, 2]);
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     * @param Definition<mixed> $expected
     */
    #[DataProvider('provideMergeCases')]
    public function test_merge(Definition $definition, Definition $other, Definition $expected): void
    {
        static::assertEquals($expected, $definition->merge($other));
    }

    public function test_merge_with_null_definition_keeps_original_type(): void
    {
        $merged = union_schema('col', type_union(type_string(), type_integer()), false)->merge(null_schema('col'));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(union_schema('col', type_union(type_string(), type_integer()), false));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, col and other');

        $def->merge(union_schema('other', type_union(type_string(), type_integer())));
    }

    public function test_merge_with_different_union_produces_json(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));
        $other = union_schema('col', type_union(type_string(), type_boolean()));

        static::assertInstanceOf(JsonDefinition::class, $def->merge($other));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        static::assertSame('string', $def->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_merge_with_non_member_falls_back_to_string(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        static::assertSame('string', $def->merge(float_schema('col'))->type()->toString());
    }

    public function test_merge_with_nullable_union_member_returns_nullable_union(): void
    {
        $merged = union_schema('col', type_union(type_string(), type_integer()), false)->merge(int_schema('col', true));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertSame('integer|string', $merged->type()->toString());
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_union_member_merges_metadata(): void
    {
        $merged = union_schema(
            'col',
            type_union(type_string(), type_integer()),
            false,
            Metadata::with('a', '1'),
        )->merge(int_schema('col', false, Metadata::with('b', '2')));

        static::assertSame('1', $merged->metadata()->get('a'));
        static::assertSame('2', $merged->metadata()->get('b'));
    }

    public function test_merge_with_union_member_returns_union(): void
    {
        $merged = union_schema('col', type_union(type_string(), type_integer()))->merge(int_schema('col'));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertSame('integer|string', $merged->type()->toString());
        static::assertFalse($merged->isNullable());
    }

    public function test_normalize(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('col', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_normalize_and_from_array_round_trip(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), true, Metadata::with('key', 'value'));

        static::assertEquals($def, definition_from_array($def->normalize()));
    }

    public function test_nullable_does_not_match_an_entry_of_a_type_outside_the_union(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), true);

        static::assertFalse($def->matches(bool_entry('col', true)));
    }

    public function test_nullable_matches_a_null_entry_with_same_name(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), true);

        static::assertTrue($def->matches(int_entry('col', null)));
    }

    public function test_nullable_matches_a_null_value_carried_by_an_entry_of_a_different_type(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), true);

        static::assertTrue($def->matches(bool_entry('col', null)));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), true);

        static::assertTrue($def->matches(int_entry('col', 1)));
    }

    public function test_rename(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        $renamed = $def->rename('new_col');

        static::assertSame('new_col', $renamed->entry()->name());
        static::assertSame('col', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_union_type(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        static::assertSame('integer|string', $def->type()->toString());
    }

    public function test_union_member_and_standalone_array_resolve_to_the_same_definition(): void
    {
        $standalone = definition_from_type('col', type_array());
        $member = definition_from_type(
            'col',
            union_schema('col', type_union(type_string(), type_array()))->type()->types()->all()[1],
        );

        static::assertInstanceOf(JsonDefinition::class, $standalone);
        static::assertInstanceOf(JsonDefinition::class, $member);
        static::assertTrue($standalone->isSame($member));
    }
}
