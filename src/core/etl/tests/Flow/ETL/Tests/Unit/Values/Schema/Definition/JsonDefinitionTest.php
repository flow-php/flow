<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema\Definition;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\JsonDefinition;
use Flow\ETL\Values\Schema\Metadata;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_json;

final class JsonDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            json_schema('data'),
            json_schema('data'),
            true,
        ];

        yield 'same type different name' => [
            json_schema('data'),
            json_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            json_schema('data', false),
            json_schema('data', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            json_schema('data', true),
            json_schema('data', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            json_schema('data'),
            json_schema('data'),
            json_schema('data'),
        ];

        yield 'makes nullable when other is nullable' => [
            json_schema('data', false),
            json_schema('data', true),
            json_schema('data', true),
        ];

        yield 'with string produces string' => [
            json_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = json_schema('data');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        static::assertFalse(json_schema('col')->matches(null));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        static::assertFalse(json_schema('col')->matches(1));
    }

    public function test_empty_array_type_is_refused_rather_than_typed_as_json(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column "data" cannot be typed as array{}');

        definition_from_type('data', type_empty_array());
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
        static::assertFalse(json_schema('data', false, Metadata::with('key', 'value1'))->isSame(json_schema(
            'data',
            false,
            Metadata::with('key', 'value2'),
        )));
    }

    public function test_is_same_with_different_nullability(): void
    {
        static::assertFalse(json_schema('data', true)->isSame(json_schema('data', false)));
    }

    public function test_is_same_with_different_type(): void
    {
        static::assertFalse(json_schema('data')->isSame(string_schema('data')));
    }

    public function test_is_same_with_identical_definition(): void
    {
        static::assertTrue(json_schema('data', true, Metadata::with('key', 'value'))->isSame(json_schema(
            'data',
            true,
            Metadata::with('key', 'value'),
        )));
    }

    public function test_make_nullable(): void
    {
        $def = json_schema('data', false);

        static::assertTrue($def->makeNullable()->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        static::assertTrue(json_schema('data')->matches(type_json()->cast(['key' => 'value'])));
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
        $merged = json_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(JsonDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(json_schema('col', false));

        static::assertInstanceOf(JsonDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = json_schema('data');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, data and other');

        $def->merge(json_schema('other'));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        static::assertSame('string', json_schema('col')->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $normalized = json_schema('data', true, Metadata::with('key', 'value'))->normalize();

        static::assertSame('data', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        static::assertFalse(json_schema('col', true)->matches(1));
    }

    public function test_nullable_matches_null(): void
    {
        static::assertTrue(json_schema('col', true)->matches(null));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        static::assertTrue(json_schema('col', true)->matches(type_json()->cast(['key' => 'value'])));
    }

    public function test_rename(): void
    {
        $def = json_schema('data');

        static::assertSame('new_data', $def->rename('new_data')->name());
        static::assertSame('data', $def->name());
    }

    public function test_set_metadata(): void
    {
        $def = json_schema('data');
        $metadata = Metadata::with('key', 'value');

        static::assertTrue($def->setMetadata($metadata)->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_json_type(): void
    {
        static::assertSame('json', json_schema('data')->type()->toString());
    }

    public function test_merge_with_an_unrelated_type_falls_back_to_common_type(): void
    {
        static::assertSame('string', json_schema('col')->merge(bool_schema('col'))->type()->toString());
    }
}
