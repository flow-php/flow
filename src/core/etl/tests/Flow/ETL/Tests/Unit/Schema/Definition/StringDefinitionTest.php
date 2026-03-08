<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use function Flow\ETL\DSL\{int_entry, int_schema, str_entry, string_schema};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class StringDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases() : \Generator
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

    public static function provideMergeCases() : \Generator
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

    public function test_add_metadata() : void
    {
        $def = string_schema('name');

        $withMeta = $def->addMetadata('key', 'value');

        self::assertTrue($withMeta->metadata()->has('key'));
        self::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name() : void
    {
        $def = string_schema('name');

        self::assertFalse($def->matches(str_entry('other', 'value')));
    }

    public function test_does_not_match_entry_with_different_type() : void
    {
        $def = string_schema('col');

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
        $def = string_schema('name', false, Metadata::with('key', 'value1'));
        $other = string_schema('name', false, Metadata::with('key', 'value2'));

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability() : void
    {
        $def = string_schema('name', true);
        $other = string_schema('name', false);

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type() : void
    {
        $def = string_schema('name');
        $other = int_schema('name');

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition() : void
    {
        $def = string_schema('name', true, Metadata::with('key', 'value'));
        $other = string_schema('name', true, Metadata::with('key', 'value'));

        self::assertTrue($def->isSame($other));
    }

    public function test_make_nullable() : void
    {
        $def = string_schema('name', false);

        $nullable = $def->makeNullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type() : void
    {
        $def = string_schema('name');

        self::assertTrue($def->matches(str_entry('name', 'value')));
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

    public function test_merge_two_assumed_nulls_keeps_from_null_metadata() : void
    {
        $nullDef1 = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $nullDef2 = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $nullDef1->merge($nullDef2);

        self::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null() : void
    {
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = string_schema('col', false);

        $merged = $nullDef->merge($def);

        self::assertInstanceOf(StringDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type() : void
    {
        $def = string_schema('col', false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        self::assertInstanceOf(StringDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception() : void
    {
        $def = string_schema('name');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, name and other');

        $def->merge(string_schema('other'));
    }

    public function test_normalize() : void
    {
        $def = string_schema('name', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        self::assertSame('name', $normalized['ref']);
        self::assertTrue($normalized['nullable']);
        self::assertArrayHasKey('type', $normalized);
        self::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name() : void
    {
        $def = string_schema('col', true);

        self::assertTrue($def->matches(str_entry('col', null)));
    }

    public function test_rename() : void
    {
        $def = string_schema('name');

        $renamed = $def->rename('new_name');

        self::assertSame('new_name', $renamed->entry()->name());
        self::assertSame('name', $def->entry()->name());
    }

    public function test_set_metadata() : void
    {
        $def = string_schema('name');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        self::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_string_type() : void
    {
        $def = string_schema('name');

        self::assertSame('string', $def->type()->toString());
    }
}
