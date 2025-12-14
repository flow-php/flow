<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use function Flow\ETL\DSL\{float_entry, float_schema, int_entry, int_schema, string_schema};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition\{BooleanDefinition, FloatDefinition};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class FloatDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases() : \Generator
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

    public static function provideMergeCases() : \Generator
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

    public static function provideMergeWithExpectedTypeCases() : \Generator
    {
        yield 'with integer produces float' => [
            float_schema('col'),
            int_schema('col'),
            FloatDefinition::class,
        ];
    }

    public function test_add_metadata() : void
    {
        $def = float_schema('amount');

        $withMeta = $def->addMetadata('key', 'value');

        self::assertTrue($withMeta->metadata()->has('key'));
        self::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name() : void
    {
        $def = float_schema('amount');

        self::assertFalse($def->matches(float_entry('other', 1.5)));
    }

    public function test_does_not_match_entry_with_different_type() : void
    {
        $def = float_schema('col');

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
        $def = float_schema('amount', false, Metadata::with('key', 'value1'));
        $other = float_schema('amount', false, Metadata::with('key', 'value2'));

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability() : void
    {
        $def = float_schema('amount', true);
        $other = float_schema('amount', false);

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type() : void
    {
        $def = float_schema('amount');
        $other = string_schema('amount');

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition() : void
    {
        $def = float_schema('amount', true, Metadata::with('key', 'value'));
        $other = float_schema('amount', true, Metadata::with('key', 'value'));

        self::assertTrue($def->isSame($other));
    }

    public function test_make_nullable() : void
    {
        $def = float_schema('amount', false);

        $nullable = $def->makeNullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type() : void
    {
        $def = float_schema('amount');

        self::assertTrue($def->matches(float_entry('amount', 1.5)));
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
        $def1 = float_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = float_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        self::assertInstanceOf(FloatDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null() : void
    {
        $nullDef = float_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = float_schema('col', false);

        $merged = $nullDef->merge($def);

        self::assertInstanceOf(FloatDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type() : void
    {
        $def = float_schema('col', false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        self::assertInstanceOf(FloatDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception() : void
    {
        $def = float_schema('amount');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, amount and other');

        $def->merge(float_schema('other'));
    }

    public function test_merge_with_incompatible_type_throws_exception() : void
    {
        $def = float_schema('col');

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize() : void
    {
        $def = float_schema('amount', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        self::assertSame('amount', $normalized['ref']);
        self::assertTrue($normalized['nullable']);
        self::assertArrayHasKey('type', $normalized);
        self::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_alias_for_make_nullable() : void
    {
        $def = float_schema('amount', false);

        $nullable = $def->nullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    public function test_nullable_matches_any_entry_with_same_name() : void
    {
        $def = float_schema('col', true);

        self::assertTrue($def->matches(float_entry('col', null)));
    }

    public function test_rename() : void
    {
        $def = float_schema('amount');

        $renamed = $def->rename('new_amount');

        self::assertSame('new_amount', $renamed->entry()->name());
        self::assertSame('amount', $def->entry()->name());
    }

    public function test_set_metadata() : void
    {
        $def = float_schema('amount');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        self::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_float_type() : void
    {
        $def = float_schema('amount');

        self::assertSame('float', $def->type()->toString());
    }
}
