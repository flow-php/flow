<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\UnsupportedUnionTypeException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\definition_from_array;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_class_string;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;

final class UnionDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            new UnionDefinition('col', type_union(type_string(), type_integer())),
            new UnionDefinition('col', type_union(type_string(), type_integer())),
            true,
        ];

        yield 'same type different name' => [
            new UnionDefinition('col', type_union(type_string(), type_integer())),
            new UnionDefinition('other', type_union(type_string(), type_integer())),
            false,
        ];

        yield 'not nullable with nullable' => [
            new UnionDefinition('col', type_union(type_string(), type_integer()), false),
            new UnionDefinition('col', type_union(type_string(), type_integer()), true),
            false,
        ];

        yield 'nullable with not nullable' => [
            new UnionDefinition('col', type_union(type_string(), type_integer()), true),
            new UnionDefinition('col', type_union(type_string(), type_integer()), false),
            true,
        ];

        yield 'different member types' => [
            new UnionDefinition('col', type_union(type_string(), type_integer())),
            new UnionDefinition('col', type_union(type_string(), type_boolean())),
            false,
        ];

        yield 'union with its string member' => [
            new UnionDefinition('col', type_union(type_string(), type_integer())),
            string_schema('col'),
            true,
        ];

        yield 'union with its integer member' => [
            new UnionDefinition('col', type_union(type_string(), type_integer())),
            int_schema('col'),
            true,
        ];

        yield 'union with a type that is not a member' => [
            new UnionDefinition('col', type_union(type_string(), type_integer())),
            float_schema('col'),
            false,
        ];

        yield 'nullable union with nullable member' => [
            new UnionDefinition('col', type_union(type_string(), type_integer()), true),
            int_schema('col', true),
            true,
        ];

        yield 'not nullable union with nullable member' => [
            new UnionDefinition('col', type_union(type_string(), type_integer()), false),
            int_schema('col', true),
            false,
        ];

        yield 'union with optional member' => [
            new UnionDefinition('col', type_union(type_optional(type_string()), type_integer())),
            string_schema('col'),
            true,
        ];

        yield 'union with a member that has no definition' => [
            new UnionDefinition('col', type_union(type_class_string(), type_string())),
            string_schema('col'),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same union' => [
            new UnionDefinition('col', type_union(type_string(), type_integer())),
            new UnionDefinition('col', type_union(type_string(), type_integer())),
            new UnionDefinition('col', type_union(type_string(), type_integer())),
        ];

        yield 'makes nullable when other is nullable' => [
            new UnionDefinition('col', type_union(type_string(), type_integer()), false),
            new UnionDefinition('col', type_union(type_string(), type_integer()), true),
            new UnionDefinition('col', type_union(type_string(), type_integer()), true),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_array_member_is_normalized_to_json(): void
    {
        static::assertSame(
            'json|string',
            (new UnionDefinition('col', type_union(type_string(), type_array())))->type()->toString(),
        );
    }

    public function test_a_union_no_longer_survives_a_normalize_round_trip(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_array()));

        $this->expectException(UnsupportedUnionTypeException::class);

        definition_from_array($def->normalize());
    }

    public function test_definition_from_type_refuses_to_create_a_union_definition(): void
    {
        $this->expectException(UnsupportedUnionTypeException::class);

        definition_from_type('col', type_union(type_string(), type_integer()));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));

        static::assertFalse($def->matches(null));
    }

    public function test_does_not_match_entry_with_value_outside_of_union(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));

        static::assertFalse($def->matches(true));
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

    public function test_a_union_hidden_in_a_list_element_is_refused_as_soon_as_the_element_is_typed(): void
    {
        // A container keeps its element Type as-is, so building the definition does not reach the
        // choke point - the refusal lands on the first call that types the element.
        $definition = definition_from_type('col', type_list(type_union(type_integer(), type_string())));

        static::assertSame('list<integer|string>', $definition->type()->toString());

        $this->expectException(UnsupportedUnionTypeException::class);

        $definition->isCompatible(definition_from_type('col', type_list(type_integer())));
    }

    public function test_is_same_with_different_metadata(): void
    {
        $def = new UnionDefinition(
            'col',
            type_union(type_string(), type_integer()),
            false,
            Metadata::with('key', 'value1'),
        );
        $other = new UnionDefinition(
            'col',
            type_union(type_string(), type_integer()),
            false,
            Metadata::with('key', 'value2'),
        );

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()), true);
        $other = new UnionDefinition('col', type_union(type_string(), type_integer()), false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));
        $other = string_schema('col');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_is_field_order_sensitive_for_structure_members(): void
    {
        $ordered = type_union(type_structure(['a' => type_integer(), 'b' => type_string()]), type_integer());
        $reordered = type_union(type_structure(['b' => type_string(), 'a' => type_integer()]), type_integer());

        static::assertFalse((new UnionDefinition('col', $ordered))->isSame(new UnionDefinition('col', $reordered)));
        static::assertFalse((new UnionDefinition('col', $ordered))->isCompatible(new UnionDefinition(
            'col',
            $reordered,
        )));

        // structure members differing only in field order are no longer the same union - the merge
        // degrades to json instead of collapsing
        static::assertInstanceOf(
            JsonDefinition::class,
            (new UnionDefinition('col', $ordered))->merge(new UnionDefinition('col', $reordered)),
        );
        static::assertInstanceOf(
            UnionDefinition::class,
            (new UnionDefinition('col', $ordered))->merge(new UnionDefinition('col', $ordered)),
        );
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = new UnionDefinition(
            'col',
            type_union(type_string(), type_integer()),
            true,
            Metadata::with('key', 'value'),
        );
        $other = new UnionDefinition(
            'col',
            type_union(type_string(), type_integer()),
            true,
            Metadata::with('key', 'value'),
        );

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()), false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_value_valid_for_any_member(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));

        static::assertTrue($def->matches('value'));
        static::assertTrue($def->matches(1));
    }

    public function test_member_for_carries_nullability_and_metadata(): void
    {
        $def = new UnionDefinition(
            'col',
            type_union(type_string(), type_integer()),
            true,
            Metadata::with('key', 'value'),
        );

        static::assertEquals(int_schema('col', true, Metadata::with('key', 'value')), $def->memberFor(1));
    }

    public function test_member_for_resolves_the_first_non_null_member_for_null(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));

        static::assertEquals(string_schema('col'), $def->memberFor(null));
    }

    public function test_member_for_resolves_the_member_accepting_the_value(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));

        static::assertEquals(int_schema('col'), $def->memberFor(1));
        static::assertEquals(string_schema('col'), $def->memberFor('value'));
    }

    public function test_member_for_throws_for_a_value_outside_every_member(): void
    {
        $def = new UnionDefinition('col', type_union(type_uuid(), type_datetime()));

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
        $merged = (new UnionDefinition('col', type_union(type_string(), type_integer()), false))->merge(null_schema(
            'col',
        ));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(
            new UnionDefinition('col', type_union(type_string(), type_integer()), false),
        );

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, col and other');

        $def->merge(new UnionDefinition('other', type_union(type_string(), type_integer())));
    }

    public function test_merge_with_different_union_produces_json(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));
        $other = new UnionDefinition('col', type_union(type_string(), type_boolean()));

        static::assertInstanceOf(JsonDefinition::class, $def->merge($other));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));

        static::assertSame('string', $def->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_merge_with_non_member_falls_back_to_string(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));

        static::assertSame('string', $def->merge(float_schema('col'))->type()->toString());
    }

    public function test_merge_with_nullable_union_member_returns_nullable_union(): void
    {
        $merged = (new UnionDefinition('col', type_union(type_string(), type_integer()), false))->merge(int_schema(
            'col',
            true,
        ));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertSame('integer|string', $merged->type()->toString());
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_union_member_merges_metadata(): void
    {
        $merged = (new UnionDefinition(
            'col',
            type_union(type_string(), type_integer()),
            false,
            Metadata::with('a', '1'),
        ))->merge(int_schema('col', false, Metadata::with('b', '2')));

        static::assertSame('1', $merged->metadata()->get('a'));
        static::assertSame('2', $merged->metadata()->get('b'));
    }

    public function test_merge_with_union_member_returns_union(): void
    {
        $merged = (new UnionDefinition('col', type_union(type_string(), type_integer())))->merge(int_schema('col'));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertSame('integer|string', $merged->type()->toString());
        static::assertFalse($merged->isNullable());
    }

    public function test_normalize(): void
    {
        $def = new UnionDefinition(
            'col',
            type_union(type_string(), type_integer()),
            true,
            Metadata::with('key', 'value'),
        );

        $normalized = $def->normalize();

        static::assertSame('col', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_normalize_still_describes_the_union_even_though_it_cannot_be_read_back(): void
    {
        $normalized = (new UnionDefinition(
            'col',
            type_union(type_string(), type_integer()),
            true,
            Metadata::with('key', 'value'),
        ))->normalize();

        static::assertSame('col', $normalized['ref']);
        static::assertTrue($normalized['nullable']);

        $this->expectException(UnsupportedUnionTypeException::class);

        definition_from_array($normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_type_outside_the_union(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()), true);

        static::assertFalse($def->matches(true));
    }

    public function test_nullable_matches_null(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()), true);

        static::assertTrue($def->matches(null));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()), true);

        static::assertTrue($def->matches(1));
    }

    public function test_rename(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));

        $renamed = $def->rename('new_col');

        static::assertSame('new_col', $renamed->entry()->name());
        static::assertSame('col', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_union_type(): void
    {
        $def = new UnionDefinition('col', type_union(type_string(), type_integer()));

        static::assertSame('integer|string', $def->type()->toString());
    }

    public function test_union_member_and_standalone_array_resolve_to_the_same_definition(): void
    {
        $standalone = definition_from_type('col', type_array());
        $member = definition_from_type(
            'col',
            (new UnionDefinition('col', type_union(type_string(), type_array())))->type()->types()->all()[1],
        );

        static::assertInstanceOf(JsonDefinition::class, $standalone);
        static::assertInstanceOf(JsonDefinition::class, $member);
        static::assertTrue($standalone->isSame($member));
    }
}
