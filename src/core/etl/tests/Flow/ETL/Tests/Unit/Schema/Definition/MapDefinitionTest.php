<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use function Flow\ETL\DSL\{int_entry, map_entry, map_schema, string_schema};
use function Flow\Types\DSL\{type_integer, type_map, type_string};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\{JsonDefinition, MapDefinition};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class MapDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases() : \Generator
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

    public static function provideMergeCases() : \Generator
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
    }

    public static function provideMergeWithExpectedTypeCases() : \Generator
    {
        yield 'different map type produces json' => [
            map_schema('col', type_map(type_string(), type_integer())),
            map_schema('col', type_map(type_string(), type_string())),
            JsonDefinition::class,
        ];
    }

    public function test_add_metadata() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        $withMeta = $def->addMetadata('key', 'value');

        self::assertTrue($withMeta->metadata()->has('key'));
        self::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        self::assertFalse($def->matches(map_entry('other', ['a' => 1], type_map(type_string(), type_integer()))));
    }

    public function test_does_not_match_entry_with_different_type() : void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()));

        self::assertFalse($def->matches(int_entry('col', 1)));
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     */
    #[DataProvider('provideIsCompatibleCases')]
    public function test_is_compatible(Definition $definition, Definition $other, bool $expected) : void
    {
        self::assertSame($expected, $definition->isCompatible($other));
    }

    public function test_is_same_with_different_metadata() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()), false, Metadata::with('key', 'value1'));
        $other = map_schema('data', type_map(type_string(), type_integer()), false, Metadata::with('key', 'value2'));

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()), true);
        $other = map_schema('data', type_map(type_string(), type_integer()), false);

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));
        $other = string_schema('data');

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()), true, Metadata::with('key', 'value'));
        $other = map_schema('data', type_map(type_string(), type_integer()), true, Metadata::with('key', 'value'));

        self::assertTrue($def->isSame($other));
    }

    public function test_make_nullable() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()), false);

        $nullable = $def->makeNullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        self::assertTrue($def->matches(map_entry('data', ['a' => 1, 'b' => 2], type_map(type_string(), type_integer()))));
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     * @param Definition<mixed> $expected
     */
    #[DataProvider('provideMergeCases')]
    public function test_merge(Definition $definition, Definition $other, Definition $expected) : void
    {
        self::assertEquals($expected, $definition->merge($other));
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     * @param class-string<object> $expectedClass
     */
    #[DataProvider('provideMergeWithExpectedTypeCases')]
    public function test_merge_produces_expected_type(Definition $definition, Definition $other, string $expectedClass) : void
    {
        self::assertInstanceOf($expectedClass, $definition->merge($other));
    }

    public function test_merge_when_both_are_from_null() : void
    {
        $def1 = map_schema('col', type_map(type_string(), type_integer()), true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = map_schema('col', type_map(type_string(), type_integer()), true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        self::assertInstanceOf(MapDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null() : void
    {
        $nullDef = map_schema('col', type_map(type_string(), type_integer()), true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = map_schema('col', type_map(type_string(), type_integer()), false);

        $merged = $nullDef->merge($def);

        self::assertInstanceOf(MapDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type() : void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()), false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        self::assertInstanceOf(MapDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, data and other');

        $def->merge(map_schema('other', type_map(type_string(), type_integer())));
    }

    public function test_merge_with_incompatible_type_throws_exception() : void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()));

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()), true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        self::assertSame('data', $normalized['ref']);
        self::assertTrue($normalized['nullable']);
        self::assertArrayHasKey('type', $normalized);
        self::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name() : void
    {
        $def = map_schema('col', type_map(type_string(), type_integer()), true);

        self::assertTrue($def->matches(map_entry('col', null, type_map(type_string(), type_integer()))));
    }

    public function test_rename() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        $renamed = $def->rename('new_data');

        self::assertSame('new_data', $renamed->entry()->name());
        self::assertSame('data', $def->entry()->name());
    }

    public function test_set_metadata() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        self::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_map_type() : void
    {
        $def = map_schema('data', type_map(type_string(), type_integer()));

        self::assertStringContainsString('map', $def->type()->toString());
    }
}
