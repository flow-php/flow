<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use function Flow\ETL\DSL\{int_entry, json_entry, json_schema, string_schema};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition\{BooleanDefinition, JsonDefinition};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class JsonDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases() : \Generator
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

    public static function provideMergeCases() : \Generator
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

    public function test_add_metadata() : void
    {
        $def = json_schema('data');

        $withMeta = $def->addMetadata('key', 'value');

        self::assertTrue($withMeta->metadata()->has('key'));
        self::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name() : void
    {
        $def = json_schema('data');

        self::assertFalse($def->matches(json_entry('other', ['key' => 'value'])));
    }

    public function test_does_not_match_entry_with_different_type() : void
    {
        $def = json_schema('col');

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
        $def = json_schema('data', false, Metadata::with('key', 'value1'));
        $other = json_schema('data', false, Metadata::with('key', 'value2'));

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability() : void
    {
        $def = json_schema('data', true);
        $other = json_schema('data', false);

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type() : void
    {
        $def = json_schema('data');
        $other = string_schema('data');

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition() : void
    {
        $def = json_schema('data', true, Metadata::with('key', 'value'));
        $other = json_schema('data', true, Metadata::with('key', 'value'));

        self::assertTrue($def->isSame($other));
    }

    public function test_make_nullable() : void
    {
        $def = json_schema('data', false);

        $nullable = $def->makeNullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type() : void
    {
        $def = json_schema('data');

        self::assertTrue($def->matches(json_entry('data', ['key' => 'value'])));
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
        $def1 = json_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = json_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        self::assertInstanceOf(JsonDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null() : void
    {
        $nullDef = json_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = json_schema('col', false);

        $merged = $nullDef->merge($def);

        self::assertInstanceOf(JsonDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type() : void
    {
        $def = json_schema('col', false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        self::assertInstanceOf(JsonDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception() : void
    {
        $def = json_schema('data');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, data and other');

        $def->merge(json_schema('other'));
    }

    public function test_merge_with_incompatible_type_throws_exception() : void
    {
        $def = json_schema('col');

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize() : void
    {
        $def = json_schema('data', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        self::assertSame('data', $normalized['ref']);
        self::assertTrue($normalized['nullable']);
        self::assertArrayHasKey('type', $normalized);
        self::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name() : void
    {
        $def = json_schema('col', true);

        self::assertTrue($def->matches(json_entry('col', null)));
    }

    public function test_rename() : void
    {
        $def = json_schema('data');

        $renamed = $def->rename('new_data');

        self::assertSame('new_data', $renamed->entry()->name());
        self::assertSame('data', $def->entry()->name());
    }

    public function test_set_metadata() : void
    {
        $def = json_schema('data');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        self::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_json_type() : void
    {
        $def = json_schema('data');

        self::assertSame('json', $def->type()->toString());
    }
}
