<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema\Definition;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\ETL\Values\Schema\Metadata;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\definition_from_array;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_string;

final class StringDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            string_schema('name'),
            string_schema('name'),
            true,
        ];

        yield 'same type different name' => [
            string_schema('name'),
            string_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            string_schema('name', false),
            string_schema('name', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            string_schema('name', true),
            string_schema('name', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            string_schema('name'),
            string_schema('name'),
            string_schema('name'),
        ];

        yield 'makes nullable when other is nullable' => [
            string_schema('name', false),
            string_schema('name', true),
            string_schema('name', true),
        ];

        yield 'with integer produces string' => [
            string_schema('col'),
            int_schema('col'),
            string_schema('col'),
        ];
    }

    public function test_a_numeric_string_definition_keeps_its_type_through_every_rebuild(): void
    {
        $def = definition_from_type('id', type_numeric_string());

        static::assertInstanceOf(StringDefinition::class, $def);
        static::assertTrue(type_equals(type_numeric_string(), $def->type()));
        static::assertTrue(type_equals(type_numeric_string(), $def->makeNullable()->type()));
        static::assertTrue(type_equals(type_numeric_string(), $def->rename('other')->type()));
        static::assertTrue(type_equals(type_numeric_string(), $def->setMetadata(Metadata::with('k', 'v'))->type()));
        static::assertTrue(type_equals(type_numeric_string(), $def->addMetadata('k', 'v')->type()));
        static::assertTrue(type_equals(
            type_numeric_string(),
            $def->merge(definition_from_type('id', type_numeric_string()))->type(),
        ));
        static::assertTrue(type_equals(type_numeric_string(), $def->merge(null_schema('id'))->type()));
        static::assertTrue(type_equals(type_string(), $def->merge(string_schema('id'))->type()));
        static::assertTrue($def->isSame(definition_from_array($def->normalize())));
    }

    public function test_add_metadata(): void
    {
        $def = string_schema('name');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        static::assertFalse(string_schema('col')->matches(null));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        static::assertFalse(string_schema('col')->matches(1));
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
        static::assertFalse(string_schema('name', false, Metadata::with('key', 'value1'))->isSame(string_schema(
            'name',
            false,
            Metadata::with('key', 'value2'),
        )));
    }

    public function test_is_same_with_different_nullability(): void
    {
        static::assertFalse(string_schema('name', true)->isSame(string_schema('name', false)));
    }

    public function test_is_same_with_different_type(): void
    {
        static::assertFalse(string_schema('name')->isSame(int_schema('name')));
    }

    public function test_is_same_with_identical_definition(): void
    {
        static::assertTrue(string_schema('name', true, Metadata::with('key', 'value'))->isSame(string_schema(
            'name',
            true,
            Metadata::with('key', 'value'),
        )));
    }

    public function test_make_nullable(): void
    {
        $def = string_schema('name', false);

        static::assertTrue($def->makeNullable()->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        static::assertTrue(string_schema('name')->matches('value'));
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
        $merged = string_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(StringDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(string_schema('col', false));

        static::assertInstanceOf(StringDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = string_schema('name');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, name and other');

        $def->merge(string_schema('other'));
    }

    public function test_normalize(): void
    {
        $normalized = string_schema('name', true, Metadata::with('key', 'value'))->normalize();

        static::assertSame('name', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        static::assertFalse(string_schema('col', true)->matches(1));
    }

    public function test_nullable_matches_null(): void
    {
        static::assertTrue(string_schema('col', true)->matches(null));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        static::assertTrue(string_schema('col', true)->matches('value'));
    }

    public function test_rename(): void
    {
        $def = string_schema('name');

        static::assertSame('new_name', $def->rename('new_name')->name());
        static::assertSame('name', $def->name());
    }

    public function test_set_metadata(): void
    {
        $def = string_schema('name');
        $metadata = Metadata::with('key', 'value');

        static::assertTrue($def->setMetadata($metadata)->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_string_type(): void
    {
        static::assertSame('string', string_schema('name')->type()->toString());
    }

    public function test_merge_with_an_unrelated_type_falls_back_to_common_type(): void
    {
        static::assertInstanceOf(StringDefinition::class, string_schema('col')->merge(bool_schema('col')));
    }

    public function test_nullability_does_not_widen_what_matches(): void
    {
        static::assertFalse(str_schema('name', nullable: true)->matches(1));
    }
}
