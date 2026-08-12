<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\ListDefinition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class ListDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            list_schema('items', type_list(type_integer())),
            list_schema('items', type_list(type_integer())),
            true,
        ];

        yield 'same type different name' => [
            list_schema('items', type_list(type_integer())),
            list_schema('other', type_list(type_integer())),
            false,
        ];

        yield 'not nullable with nullable' => [
            list_schema('items', type_list(type_integer()), false),
            list_schema('items', type_list(type_integer()), true),
            false,
        ];

        yield 'nullable with not nullable' => [
            list_schema('items', type_list(type_integer()), true),
            list_schema('items', type_list(type_integer()), false),
            true,
        ];

        yield 'different element type' => [
            list_schema('items', type_list(type_integer())),
            list_schema('items', type_list(type_string())),
            false,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            list_schema('items', type_list(type_integer())),
            list_schema('items', type_list(type_integer())),
            list_schema('items', type_list(type_integer())),
        ];

        yield 'makes nullable when other is nullable' => [
            list_schema('items', type_list(type_integer()), false),
            list_schema('items', type_list(type_integer()), true),
            list_schema('items', type_list(type_integer()), true),
        ];

        yield 'with string produces string' => [
            list_schema('col', type_list(type_integer())),
            string_schema('col'),
            string_schema('col'),
        ];

        yield 'integer list with float list produces float list' => [
            list_schema('col', type_list(type_integer())),
            list_schema('col', type_list(type_float())),
            list_schema('col', type_list(type_float())),
        ];

        yield 'different element type widens the element, staying a list' => [
            list_schema('col', type_list(type_integer())),
            list_schema('col', type_list(type_string())),
            list_schema('col', type_list(type_string())),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = list_schema('items', type_list(type_integer()));

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_map_entry_holding_a_list_shaped_value(): void
    {
        $def = list_schema('col', type_list(type_integer()));

        static::assertFalse($def->matches(map_entry('col', [1, 2], type_map(type_integer(), type_integer()))));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        $def = list_schema('col', type_list(type_integer()));

        static::assertFalse($def->matches(list_entry('col', null, type_list(type_integer()))));
    }

    public function test_does_not_match_an_entry_of_a_different_list_instantiation(): void
    {
        $def = list_schema('col', type_list(type_integer()));

        static::assertFalse($def->matches(list_entry('col', ['x'], type_list(type_string()))));
    }

    public function test_does_not_match_an_entry_of_a_different_nested_list_instantiation(): void
    {
        $def = list_schema('col', type_list(type_list(type_integer())));

        static::assertFalse($def->matches(list_entry('col', [['x']], type_list(type_list(type_string())))));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = list_schema('items', type_list(type_integer()));

        static::assertFalse($def->matches(list_entry('other', [1, 2, 3], type_list(type_integer()))));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = list_schema('col', type_list(type_integer()));

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_array_element_is_projected_to_json(): void
    {
        static::assertSame('list<json>', list_schema('items', type_list(type_array()))->type()->toString());
    }

    public function test_empty_array_element_is_projected_to_json(): void
    {
        static::assertSame('list<json>', list_schema('items', type_list(type_empty_array()))->type()->toString());
    }

    public function test_entry_class(): void
    {
        static::assertSame(ListEntry::class, list_schema('items', type_list(type_integer()))->entryClass());
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

    public function test_is_same_with_different_metadata(): void
    {
        $def = list_schema('items', type_list(type_integer()), false, Metadata::with('key', 'value1'));
        $other = list_schema('items', type_list(type_integer()), false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = list_schema('items', type_list(type_integer()), true);
        $other = list_schema('items', type_list(type_integer()), false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = list_schema('items', type_list(type_integer()));
        $other = string_schema('items');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = list_schema('items', type_list(type_integer()), true, Metadata::with('key', 'value'));
        $other = list_schema('items', type_list(type_integer()), true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = list_schema('items', type_list(type_integer()), false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_an_empty_list_entry_of_a_different_instantiation(): void
    {
        $def = list_schema('col', type_list(type_integer()));

        static::assertTrue($def->matches(list_entry('col', [], type_list(type_string()))));
    }

    public function test_matches_and_is_compatible_agree_on_a_different_instantiation(): void
    {
        $def = list_schema('col', type_list(type_integer()));

        static::assertFalse($def->matches(list_entry('col', ['x'], type_list(type_string()))));
        static::assertFalse($def->isCompatible(list_schema('col', type_list(type_string()))));
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = list_schema('items', type_list(type_integer()));

        static::assertTrue($def->matches(list_entry('items', [1, 2, 3], type_list(type_integer()))));
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
        $merged = list_schema('col', type_list(type_integer()), false)->merge(null_schema('col'));

        static::assertInstanceOf(ListDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(list_schema('col', type_list(type_integer()), false));

        static::assertInstanceOf(ListDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = list_schema('items', type_list(type_integer()));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, items and other');

        $def->merge(list_schema('other', type_list(type_integer())));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        $def = list_schema('col', type_list(type_integer()));

        static::assertSame('string', $def->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $def = list_schema('items', type_list(type_integer()), true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('items', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        $def = list_schema('col', type_list(type_integer()), true);

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_nullable_matches_a_null_entry_with_same_name(): void
    {
        $def = list_schema('col', type_list(type_integer()), true);

        static::assertTrue($def->matches(list_entry('col', null, type_list(type_integer()))));
    }

    public function test_nullable_matches_a_null_value_carried_by_an_entry_of_a_different_type(): void
    {
        $def = list_schema('col', type_list(type_integer()), true);

        static::assertTrue($def->matches(int_entry('col', null)));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        $def = list_schema('col', type_list(type_integer()), true);

        static::assertTrue($def->matches(list_entry('col', [1, 2, 3], type_list(type_integer()))));
    }

    public function test_rename(): void
    {
        $def = list_schema('items', type_list(type_integer()));

        $renamed = $def->rename('new_items');

        static::assertSame('new_items', $renamed->entry()->name());
        static::assertSame('items', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = list_schema('items', type_list(type_integer()));
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_list_type(): void
    {
        $def = list_schema('items', type_list(type_integer()));

        static::assertStringContainsString('list', $def->type()->toString());
    }

    public function test_merge_with_union_containing_this_type_returns_union(): void
    {
        $merged = list_schema('col', type_list(type_integer()))->merge(union_schema('col', type_union(
            type_list(type_integer()),
            type_boolean(),
        )));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertSame('boolean|list<integer>', $merged->type()->toString());
    }

    public function test_merge_with_union_not_containing_this_type_falls_back_to_string(): void
    {
        static::assertSame(
            'string',
            list_schema('col', type_list(type_integer()))
                ->merge(union_schema('col', type_union(type_integer(), type_string())))
                ->type()
                ->toString(),
        );
    }
}
