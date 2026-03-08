<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use function Flow\ETL\DSL\{bool_entry, bool_schema, int_entry, string_schema};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition\{BooleanDefinition, IntegerDefinition};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class BooleanDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases() : \Generator
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

    public static function provideMergeCases() : \Generator
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

    public function test_add_metadata() : void
    {
        $def = bool_schema('flag');

        $withMeta = $def->addMetadata('key', 'value');

        self::assertTrue($withMeta->metadata()->has('key'));
        self::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name() : void
    {
        $def = bool_schema('flag');

        self::assertFalse($def->matches(bool_entry('other', true)));
    }

    public function test_does_not_match_entry_with_different_type() : void
    {
        $def = bool_schema('col');

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
        $def = bool_schema('flag', false, Metadata::with('key', 'value1'));
        $other = bool_schema('flag', false, Metadata::with('key', 'value2'));

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability() : void
    {
        $def = bool_schema('flag', true);
        $other = bool_schema('flag', false);

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type() : void
    {
        $def = bool_schema('flag');
        $other = string_schema('flag');

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition() : void
    {
        $def = bool_schema('flag', true, Metadata::with('key', 'value'));
        $other = bool_schema('flag', true, Metadata::with('key', 'value'));

        self::assertTrue($def->isSame($other));
    }

    public function test_make_nullable() : void
    {
        $def = bool_schema('flag', false);

        $nullable = $def->makeNullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type() : void
    {
        $def = bool_schema('flag');

        self::assertTrue($def->matches(bool_entry('flag', true)));
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

    public function test_merge_when_both_are_from_null() : void
    {
        $def1 = bool_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = bool_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        self::assertInstanceOf(BooleanDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null() : void
    {
        $nullDef = bool_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = bool_schema('col', false);

        $merged = $nullDef->merge($def);

        self::assertInstanceOf(BooleanDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type() : void
    {
        $def = bool_schema('col', false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        self::assertInstanceOf(BooleanDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception() : void
    {
        $def = bool_schema('flag');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, flag and other');

        $def->merge(bool_schema('other'));
    }

    public function test_merge_with_incompatible_type_throws_exception() : void
    {
        $def = bool_schema('col');

        $this->expectException(RuntimeException::class);

        $def->merge(new IntegerDefinition('col'));
    }

    public function test_normalize() : void
    {
        $def = bool_schema('flag', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        self::assertSame('flag', $normalized['ref']);
        self::assertTrue($normalized['nullable']);
        self::assertArrayHasKey('type', $normalized);
        self::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name() : void
    {
        $def = bool_schema('col', true);

        self::assertTrue($def->matches(bool_entry('col', null)));
    }

    public function test_rename() : void
    {
        $def = bool_schema('flag');

        $renamed = $def->rename('new_name');

        self::assertSame('new_name', $renamed->entry()->name());
        self::assertSame('flag', $def->entry()->name());
    }

    public function test_set_metadata() : void
    {
        $def = bool_schema('flag');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        self::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_boolean_type() : void
    {
        $def = bool_schema('flag');

        self::assertSame('boolean', $def->type()->toString());
    }
}
