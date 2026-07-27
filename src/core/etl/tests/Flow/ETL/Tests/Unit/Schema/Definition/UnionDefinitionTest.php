<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

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
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

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

        yield 'union with non union' => [
            union_schema('col', type_union(type_string(), type_integer())),
            string_schema('col'),
            false,
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

    public function test_definition_from_type_creates_union_definition(): void
    {
        $definition = definition_from_type('col', type_union(type_string(), type_integer()));

        static::assertInstanceOf(UnionDefinition::class, $definition);
        static::assertSame('col', $definition->entry()->name());
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

    public function test_merge_with_incompatible_type_throws_exception(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()));

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
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

    public function test_nullable_matches_any_entry_with_same_name(): void
    {
        $def = union_schema('col', type_union(type_string(), type_integer()), true);

        static::assertTrue($def->matches(bool_entry('col', true)));
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
}
