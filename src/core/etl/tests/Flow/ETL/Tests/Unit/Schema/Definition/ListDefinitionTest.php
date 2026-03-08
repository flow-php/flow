<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use function Flow\ETL\DSL\{int_entry, list_entry, list_schema, string_schema};
use function Flow\Types\DSL\{type_float, type_integer, type_list, type_string};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\{JsonDefinition, ListDefinition};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class ListDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases() : \Generator
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

    public static function provideMergeCases() : \Generator
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
    }

    public static function provideMergeWithExpectedTypeCases() : \Generator
    {
        yield 'integer list with float list produces float list' => [
            list_schema('col', type_list(type_integer())),
            list_schema('col', type_list(type_float())),
            ListDefinition::class,
        ];

        yield 'different element type produces json' => [
            list_schema('col', type_list(type_integer())),
            list_schema('col', type_list(type_string())),
            JsonDefinition::class,
        ];
    }

    public function test_add_metadata() : void
    {
        $def = list_schema('items', type_list(type_integer()));

        $withMeta = $def->addMetadata('key', 'value');

        self::assertTrue($withMeta->metadata()->has('key'));
        self::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name() : void
    {
        $def = list_schema('items', type_list(type_integer()));

        self::assertFalse($def->matches(list_entry('other', [1, 2, 3], type_list(type_integer()))));
    }

    public function test_does_not_match_entry_with_different_type() : void
    {
        $def = list_schema('col', type_list(type_integer()));

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
        $def = list_schema('items', type_list(type_integer()), false, Metadata::with('key', 'value1'));
        $other = list_schema('items', type_list(type_integer()), false, Metadata::with('key', 'value2'));

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability() : void
    {
        $def = list_schema('items', type_list(type_integer()), true);
        $other = list_schema('items', type_list(type_integer()), false);

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type() : void
    {
        $def = list_schema('items', type_list(type_integer()));
        $other = string_schema('items');

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition() : void
    {
        $def = list_schema('items', type_list(type_integer()), true, Metadata::with('key', 'value'));
        $other = list_schema('items', type_list(type_integer()), true, Metadata::with('key', 'value'));

        self::assertTrue($def->isSame($other));
    }

    public function test_make_nullable() : void
    {
        $def = list_schema('items', type_list(type_integer()), false);

        $nullable = $def->makeNullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type() : void
    {
        $def = list_schema('items', type_list(type_integer()));

        self::assertTrue($def->matches(list_entry('items', [1, 2, 3], type_list(type_integer()))));
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
        $def1 = list_schema('col', type_list(type_integer()), true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = list_schema('col', type_list(type_integer()), true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        self::assertInstanceOf(ListDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null() : void
    {
        $nullDef = list_schema('col', type_list(type_integer()), true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = list_schema('col', type_list(type_integer()), false);

        $merged = $nullDef->merge($def);

        self::assertInstanceOf(ListDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type() : void
    {
        $def = list_schema('col', type_list(type_integer()), false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        self::assertInstanceOf(ListDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception() : void
    {
        $def = list_schema('items', type_list(type_integer()));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, items and other');

        $def->merge(list_schema('other', type_list(type_integer())));
    }

    public function test_merge_with_incompatible_type_throws_exception() : void
    {
        $def = list_schema('col', type_list(type_integer()));

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize() : void
    {
        $def = list_schema('items', type_list(type_integer()), true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        self::assertSame('items', $normalized['ref']);
        self::assertTrue($normalized['nullable']);
        self::assertArrayHasKey('type', $normalized);
        self::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name() : void
    {
        $def = list_schema('col', type_list(type_integer()), true);

        self::assertTrue($def->matches(list_entry('col', null, type_list(type_integer()))));
    }

    public function test_rename() : void
    {
        $def = list_schema('items', type_list(type_integer()));

        $renamed = $def->rename('new_items');

        self::assertSame('new_items', $renamed->entry()->name());
        self::assertSame('items', $def->entry()->name());
    }

    public function test_set_metadata() : void
    {
        $def = list_schema('items', type_list(type_integer()));
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        self::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_list_type() : void
    {
        $def = list_schema('items', type_list(type_integer()));

        self::assertStringContainsString('list', $def->type()->toString());
    }
}
