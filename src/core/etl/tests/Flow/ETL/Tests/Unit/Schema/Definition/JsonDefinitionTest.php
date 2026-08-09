<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

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
        $def = json_schema('col');

        static::assertFalse($def->matches(json_entry('col', null)));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = json_schema('data');

        static::assertFalse($def->matches(json_entry('other', ['key' => 'value'])));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = json_schema('col');

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_entry_class(): void
    {
        static::assertSame(JsonEntry::class, json_schema('data')->entryClass());
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
        $def = json_schema('data', false, Metadata::with('key', 'value1'));
        $other = json_schema('data', false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = json_schema('data', true);
        $other = json_schema('data', false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = json_schema('data');
        $other = string_schema('data');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = json_schema('data', true, Metadata::with('key', 'value'));
        $other = json_schema('data', true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = json_schema('data', false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = json_schema('data');

        static::assertTrue($def->matches(json_entry('data', ['key' => 'value'])));
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
        $def = json_schema('col');

        static::assertSame('string', $def->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $def = json_schema('data', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('data', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        $def = json_schema('col', true);

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_nullable_matches_a_null_entry_with_same_name(): void
    {
        $def = json_schema('col', true);

        static::assertTrue($def->matches(json_entry('col', null)));
    }

    public function test_nullable_matches_a_null_value_carried_by_an_entry_of_a_different_type(): void
    {
        $def = json_schema('col', true);

        static::assertTrue($def->matches(int_entry('col', null)));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        $def = json_schema('col', true);

        static::assertTrue($def->matches(json_entry('col', ['key' => 'value'])));
    }

    public function test_rename(): void
    {
        $def = json_schema('data');

        $renamed = $def->rename('new_data');

        static::assertSame('new_data', $renamed->entry()->name());
        static::assertSame('data', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = json_schema('data');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_json_type(): void
    {
        $def = json_schema('data');

        static::assertSame('json', $def->type()->toString());
    }

    public function test_merge_with_union_containing_this_type_returns_union(): void
    {
        $merged = json_schema('col')->merge(union_schema('col', type_union(type_json(), type_boolean())));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertSame('boolean|json', $merged->type()->toString());
    }

    public function test_merge_with_union_not_containing_this_type_falls_back_to_string(): void
    {
        static::assertSame(
            'string',
            json_schema('col')
                ->merge(union_schema('col', type_union(type_integer(), type_string())))
                ->type()
                ->toString(),
        );
    }
}
