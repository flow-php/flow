<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\FloatDefinition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class FloatDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            float_schema('amount'),
            float_schema('amount'),
            true,
        ];

        yield 'same type different name' => [
            float_schema('amount'),
            float_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            float_schema('amount', false),
            float_schema('amount', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            float_schema('amount', true),
            float_schema('amount', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            float_schema('amount'),
            float_schema('amount'),
            float_schema('amount'),
        ];

        yield 'makes nullable when other is nullable' => [
            float_schema('amount', false),
            float_schema('amount', true),
            float_schema('amount', true),
        ];

        yield 'with string produces string' => [
            float_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public static function provideMergeWithExpectedTypeCases(): Generator
    {
        yield 'with integer produces float' => [
            float_schema('col'),
            int_schema('col'),
            FloatDefinition::class,
        ];
    }

    public function test_add_metadata(): void
    {
        $def = float_schema('amount');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        $def = float_schema('col');

        static::assertFalse($def->matches(float_entry('col', null)));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = float_schema('amount');

        static::assertFalse($def->matches(float_entry('other', 1.5)));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = float_schema('col');

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_entry_class(): void
    {
        static::assertSame(FloatEntry::class, float_schema('amount')->entryClass());
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
        $def = float_schema('amount', false, Metadata::with('key', 'value1'));
        $other = float_schema('amount', false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = float_schema('amount', true);
        $other = float_schema('amount', false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = float_schema('amount');
        $other = string_schema('amount');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = float_schema('amount', true, Metadata::with('key', 'value'));
        $other = float_schema('amount', true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = float_schema('amount', false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = float_schema('amount');

        static::assertTrue($def->matches(float_entry('amount', 1.5)));
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
        $merged = float_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(FloatDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(float_schema('col', false));

        static::assertInstanceOf(FloatDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = float_schema('amount');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, amount and other');

        $def->merge(float_schema('other'));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        $def = float_schema('col');

        static::assertSame('string', $def->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $def = float_schema('amount', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('amount', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        $def = float_schema('col', true);

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_nullable_matches_a_null_entry_with_same_name(): void
    {
        $def = float_schema('col', true);

        static::assertTrue($def->matches(float_entry('col', null)));
    }

    public function test_nullable_matches_a_null_value_carried_by_an_entry_of_a_different_type(): void
    {
        $def = float_schema('col', true);

        static::assertTrue($def->matches(int_entry('col', null)));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        $def = float_schema('col', true);

        static::assertTrue($def->matches(float_entry('col', 1.5)));
    }

    public function test_rename(): void
    {
        $def = float_schema('amount');

        $renamed = $def->rename('new_amount');

        static::assertSame('new_amount', $renamed->entry()->name());
        static::assertSame('amount', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = float_schema('amount');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_float_type(): void
    {
        $def = float_schema('amount');

        static::assertSame('float', $def->type()->toString());
    }

    public function test_merge_with_union_containing_this_type_returns_union(): void
    {
        $merged = float_schema('col')->merge(union_schema('col', type_union(type_float(), type_boolean())));

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertSame('boolean|float', $merged->type()->toString());
    }

    public function test_merge_with_union_not_containing_this_type_falls_back_to_string(): void
    {
        static::assertSame(
            'string',
            float_schema('col')
                ->merge(union_schema('col', type_union(type_integer(), type_string())))
                ->type()
                ->toString(),
        );
    }
}
