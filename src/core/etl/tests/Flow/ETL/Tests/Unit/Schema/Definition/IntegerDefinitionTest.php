<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\FloatDefinition;
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_schema;

final class IntegerDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            int_schema('id'),
            int_schema('id'),
            true,
        ];

        yield 'same type different name' => [
            int_schema('id'),
            int_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            int_schema('id', false),
            int_schema('id', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            int_schema('id', true),
            int_schema('id', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            int_schema('id'),
            int_schema('id'),
            int_schema('id'),
        ];

        yield 'makes nullable when other is nullable' => [
            int_schema('id', false),
            int_schema('id', true),
            int_schema('id', true),
        ];

        yield 'with string produces string' => [
            int_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public static function provideMergeWithExpectedTypeCases(): Generator
    {
        yield 'with float produces float' => [
            int_schema('col'),
            float_schema('col'),
            FloatDefinition::class,
        ];
    }

    public function test_add_metadata(): void
    {
        $def = int_schema('id');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = int_schema('id');

        static::assertFalse($def->matches(int_entry('other', 1)));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = int_schema('col');

        static::assertFalse($def->matches(str_entry('col', 'value')));
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
        $def = int_schema('id', false, Metadata::with('key', 'value1'));
        $other = int_schema('id', false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = int_schema('id', true);
        $other = int_schema('id', false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = int_schema('id');
        $other = string_schema('id');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = int_schema('id', true, Metadata::with('key', 'value'));
        $other = int_schema('id', true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = int_schema('id', false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = int_schema('id');

        static::assertTrue($def->matches(int_entry('id', 1)));
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

    public function test_merge_when_both_are_from_null(): void
    {
        $def1 = int_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = int_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        static::assertInstanceOf(IntegerDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
        static::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null(): void
    {
        $nullDef = int_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = int_schema('col', false);

        $merged = $nullDef->merge($def);

        static::assertInstanceOf(IntegerDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
        static::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type(): void
    {
        $def = int_schema('col', false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        static::assertInstanceOf(IntegerDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = int_schema('id');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, id and other');

        $def->merge(int_schema('other'));
    }

    public function test_merge_with_incompatible_type_throws_exception(): void
    {
        $def = int_schema('col');

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize(): void
    {
        $def = int_schema('id', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('id', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name(): void
    {
        $def = int_schema('col', true);

        static::assertTrue($def->matches(int_entry('col', null)));
    }

    public function test_rename(): void
    {
        $def = int_schema('id');

        $renamed = $def->rename('new_id');

        static::assertSame('new_id', $renamed->entry()->name());
        static::assertSame('id', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = int_schema('id');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_integer_type(): void
    {
        $def = int_schema('id');

        static::assertSame('integer', $def->type()->toString());
    }
}
