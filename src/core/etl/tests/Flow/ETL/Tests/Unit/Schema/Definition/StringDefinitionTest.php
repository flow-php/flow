<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

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
        $def = string_schema('col');

        static::assertFalse($def->matches(str_entry('col', null)));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = string_schema('name');

        static::assertFalse($def->matches(str_entry('other', 'value')));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = string_schema('col');

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_entry_class(): void
    {
        static::assertSame(StringEntry::class, string_schema('name')->entryClass());
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
        $def = string_schema('name', false, Metadata::with('key', 'value1'));
        $other = string_schema('name', false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = string_schema('name', true);
        $other = string_schema('name', false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = string_schema('name');
        $other = int_schema('name');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = string_schema('name', true, Metadata::with('key', 'value'));
        $other = string_schema('name', true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = string_schema('name', false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = string_schema('name');

        static::assertTrue($def->matches(str_entry('name', 'value')));
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
        $def = string_schema('name', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('name', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        $def = string_schema('col', true);

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_nullable_matches_a_null_entry_with_same_name(): void
    {
        $def = string_schema('col', true);

        static::assertTrue($def->matches(str_entry('col', null)));
    }

    public function test_nullable_matches_a_null_value_carried_by_an_entry_of_a_different_type(): void
    {
        $def = string_schema('col', true);

        static::assertTrue($def->matches(int_entry('col', null)));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        $def = string_schema('col', true);

        static::assertTrue($def->matches(str_entry('col', 'value')));
    }

    public function test_rename(): void
    {
        $def = string_schema('name');

        $renamed = $def->rename('new_name');

        static::assertSame('new_name', $renamed->entry()->name());
        static::assertSame('name', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = string_schema('name');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_string_type(): void
    {
        $def = string_schema('name');

        static::assertSame('string', $def->type()->toString());
    }

    public function test_merge_with_union_containing_this_type_returns_union(): void
    {
        $merged = string_schema('col')->merge(union_schema('col', type_union(type_string(), type_boolean())));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertSame('boolean|string', $merged->type()->toString());
    }

    public function test_merge_with_union_not_containing_string_returns_string(): void
    {
        static::assertInstanceOf(
            StringDefinition::class,
            string_schema('col')->merge(union_schema('col', type_union(type_boolean(), type_integer()))),
        );
    }
}
