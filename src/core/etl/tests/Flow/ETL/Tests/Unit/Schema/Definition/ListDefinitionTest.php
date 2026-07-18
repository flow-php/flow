<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Definition\ListDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

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
    }

    public static function provideMergeWithExpectedTypeCases(): Generator
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

    public function test_add_metadata(): void
    {
        $def = list_schema('items', type_list(type_integer()));

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
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

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     * @param class-string $expectedClass
     */
    #[DataProvider('provideMergeWithExpectedTypeCases')]
    public function test_merge_produces_expected_type(
        Definition $definition,
        Definition $other,
        string $expectedClass,
    ): void {
        static::assertInstanceOf($expectedClass, $definition->merge($other));
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

    public function test_merge_with_incompatible_type_throws_exception(): void
    {
        $def = list_schema('col', type_list(type_integer()));

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
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

    public function test_nullable_matches_any_entry_with_same_name(): void
    {
        $def = list_schema('col', type_list(type_integer()), true);

        static::assertTrue($def->matches(list_entry('col', null, type_list(type_integer()))));
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
    }

    public function test_type_returns_list_type(): void
    {
        $def = list_schema('items', type_list(type_integer()));

        static::assertStringContainsString('list', $def->type()->toString());
    }
}
