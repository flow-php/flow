<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\MapDefinition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class MapDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            map_schema('data', type_map(type_string(), type_integer())),
            map_schema('data', type_map(type_string(), type_integer())),
            true,
        ];

        yield 'same type different name' => [
            map_schema('data', type_map(type_string(), type_integer())),
            map_schema('other', type_map(type_string(), type_integer())),
            false,
        ];

        yield 'not nullable with nullable' => [
            map_schema('data', type_map(type_string(), type_integer()), false),
            map_schema('data', type_map(type_string(), type_integer()), true),
            false,
        ];

        yield 'nullable with not nullable' => [
            map_schema('data', type_map(type_string(), type_integer()), true),
            map_schema('data', type_map(type_string(), type_integer()), false),
            true,
        ];

        yield 'different key type' => [
            map_schema('data', type_map(type_string(), type_integer())),
            map_schema('data', type_map(type_integer(), type_integer())),
            false,
        ];

        yield 'different value type' => [
            map_schema('data', type_map(type_string(), type_integer())),
            map_schema('data', type_map(type_string(), type_string())),
            false,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            map_schema('data', type_map(type_string(), type_integer())),
            map_schema('data', type_map(type_string(), type_integer())),
            map_schema('data', type_map(type_string(), type_integer())),
        ];

        yield 'makes nullable when other is nullable' => [
            map_schema('data', type_map(type_string(), type_integer()), false),
            map_schema('data', type_map(type_string(), type_integer()), true),
            map_schema('data', type_map(type_string(), type_integer()), true),
        ];

        yield 'with string produces string' => [
            map_schema('col', type_map(type_string(), type_integer())),
            string_schema('col'),
            string_schema('col'),
        ];

        yield 'different value type widens the value, staying a map' => [
            map_schema('col', type_map(type_string(), type_integer())),
            map_schema('col', type_map(type_string(), type_string())),
            map_schema('col', type_map(type_string(), type_string())),
        ];

        yield 'numeric value types promote to float' => [
            map_schema('col', type_map(type_string(), type_integer())),
            map_schema('col', type_map(type_string(), type_float())),
            map_schema('col', type_map(type_string(), type_float())),
        ];

        yield 'different key type widens the key to string' => [
            map_schema('col', type_map(type_integer(), type_integer())),
            map_schema('col', type_map(type_string(), type_integer())),
            map_schema('col', type_map(type_string(), type_integer())),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_list_entry_holding_a_map_shaped_value(): void
    {
        $def = map_schema('col', type_map(type_integer(), type_integer()));

        static::assertFalse($def->matches(list_entry('col', [1, 2], type_list(type_integer()))));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()));

        static::assertFalse($def->matches(map_entry('col', null, type_map(type_string(), type_integer()))));
    }

    public function test_does_not_match_an_entry_of_a_different_map_instantiation(): void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()));

        static::assertFalse($def->matches(map_entry('col', ['a' => 'x'], type_map(type_string(), type_string()))));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        static::assertFalse($def->matches(map_entry('other', ['a' => 1], type_map(type_string(), type_integer()))));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()));

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_array_value_is_projected_to_json(): void
    {
        static::assertSame(
            'map<string, json>',
            map_schema('data', type_map(type_string(), type_array()))->type()->toString(),
        );
    }

    public function test_empty_array_value_is_projected_to_json(): void
    {
        static::assertSame(
            'map<string, json>',
            map_schema('data', type_map(type_string(), type_empty_array()))->type()->toString(),
        );
    }

    public function test_entry_class(): void
    {
        static::assertSame(MapEntry::class, map_schema('data', type_map(type_string(), type_integer()))->entryClass());
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
        $def = map_schema('data', type_map(type_string(), type_integer()), false, Metadata::with('key', 'value1'));
        $other = map_schema('data', type_map(type_string(), type_integer()), false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()), true);
        $other = map_schema('data', type_map(type_string(), type_integer()), false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));
        $other = string_schema('data');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()), true, Metadata::with('key', 'value'));
        $other = map_schema('data', type_map(type_string(), type_integer()), true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()), false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_an_empty_map_entry_of_a_different_instantiation(): void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()));

        static::assertTrue($def->matches(map_entry('col', [], type_map(type_string(), type_string()))));
    }

    public function test_matches_and_is_compatible_agree_on_a_different_instantiation(): void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()));

        static::assertFalse($def->matches(map_entry('col', ['a' => 'x'], type_map(type_string(), type_string()))));
        static::assertFalse($def->isCompatible(map_schema('col', type_map(type_string(), type_string()))));
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        static::assertTrue($def->matches(map_entry(
            'data',
            ['a' => 1, 'b' => 2],
            type_map(type_string(), type_integer()),
        )));
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
        $merged = map_schema('col', type_map(type_string(), type_integer()), false)->merge(null_schema('col'));

        static::assertInstanceOf(MapDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(map_schema('col', type_map(type_string(), type_integer()), false));

        static::assertInstanceOf(MapDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, data and other');

        $def->merge(map_schema('other', type_map(type_string(), type_integer())));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()));

        static::assertSame('string', $def->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()), true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('data', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()), true);

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_nullable_matches_a_null_entry_with_same_name(): void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()), true);

        static::assertTrue($def->matches(map_entry('col', null, type_map(type_string(), type_integer()))));
    }

    public function test_nullable_matches_a_null_value_carried_by_an_entry_of_a_different_type(): void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()), true);

        static::assertTrue($def->matches(int_entry('col', null)));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()), true);

        static::assertTrue($def->matches(map_entry('col', ['a' => 1], type_map(type_string(), type_integer()))));
    }

    public function test_rename(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        $renamed = $def->rename('new_data');

        static::assertSame('new_data', $renamed->entry()->name());
        static::assertSame('data', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_map_type(): void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        static::assertStringContainsString('map', $def->type()->toString());
    }

    public function test_merge_with_union_containing_this_type_returns_union(): void
    {
        $merged = map_schema('col', type_map(type_string(), type_integer()))->merge(
            new UnionDefinition('col', type_union(type_map(type_string(), type_integer()), type_boolean())),
        );

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertSame('boolean|map<string, integer>', $merged->type()->toString());
    }

    public function test_merge_with_union_not_containing_this_type_falls_back_to_string(): void
    {
        static::assertSame(
            'string',
            map_schema('col', type_map(type_string(), type_integer()))
                ->merge(new UnionDefinition('col', type_union(type_integer(), type_string())))
                ->type()
                ->toString(),
        );
    }
}
