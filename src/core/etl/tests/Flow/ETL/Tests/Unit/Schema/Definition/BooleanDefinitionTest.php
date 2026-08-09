<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class BooleanDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            bool_schema('flag'),
            bool_schema('flag'),
            true,
        ];

        yield 'same type different name' => [
            bool_schema('flag'),
            bool_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            bool_schema('flag', false),
            bool_schema('flag', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            bool_schema('flag', true),
            bool_schema('flag', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            bool_schema('flag'),
            bool_schema('flag'),
            bool_schema('flag'),
        ];

        yield 'makes nullable when other is nullable' => [
            bool_schema('flag', false),
            bool_schema('flag', true),
            bool_schema('flag', true),
        ];

        yield 'with string produces string' => [
            bool_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = bool_schema('flag');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        $def = bool_schema('col');

        static::assertFalse($def->matches(bool_entry('col', null)));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = bool_schema('flag');

        static::assertFalse($def->matches(bool_entry('other', true)));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = bool_schema('col');

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_entry_class(): void
    {
        static::assertSame(BooleanEntry::class, bool_schema('flag')->entryClass());
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
        $def = bool_schema('flag', false, Metadata::with('key', 'value1'));
        $other = bool_schema('flag', false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = bool_schema('flag', true);
        $other = bool_schema('flag', false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = bool_schema('flag');
        $other = string_schema('flag');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = bool_schema('flag', true, Metadata::with('key', 'value'));
        $other = bool_schema('flag', true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = bool_schema('flag', false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = bool_schema('flag');

        static::assertTrue($def->matches(bool_entry('flag', true)));
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
        $merged = bool_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(BooleanDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(bool_schema('col', false));

        static::assertInstanceOf(BooleanDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = bool_schema('flag');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, flag and other');

        $def->merge(bool_schema('other'));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        $def = bool_schema('col');

        static::assertSame('string', $def->merge(new IntegerDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $def = bool_schema('flag', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('flag', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        $def = bool_schema('col', true);

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_nullable_matches_a_null_entry_with_same_name(): void
    {
        $def = bool_schema('col', true);

        static::assertTrue($def->matches(bool_entry('col', null)));
    }

    public function test_nullable_matches_a_null_value_carried_by_an_entry_of_a_different_type(): void
    {
        $def = bool_schema('col', true);

        static::assertTrue($def->matches(int_entry('col', null)));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        $def = bool_schema('col', true);

        static::assertTrue($def->matches(bool_entry('col', true)));
    }

    public function test_rename(): void
    {
        $def = bool_schema('flag');

        $renamed = $def->rename('new_name');

        static::assertSame('new_name', $renamed->entry()->name());
        static::assertSame('flag', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = bool_schema('flag');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_boolean_type(): void
    {
        $def = bool_schema('flag');

        static::assertSame('boolean', $def->type()->toString());
    }

    public function test_merge_with_union_containing_this_type_returns_union(): void
    {
        $merged = bool_schema('col')->merge(union_schema('col', type_union(type_boolean(), type_integer())));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertSame('boolean|integer', $merged->type()->toString());
    }

    public function test_merge_with_union_not_containing_this_type_falls_back_to_string(): void
    {
        static::assertSame(
            'string',
            bool_schema('col')
                ->merge(union_schema('col', type_union(type_integer(), type_string())))
                ->type()
                ->toString(),
        );
    }
}
