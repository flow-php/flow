<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use function Flow\ETL\DSL\{int_entry, string_schema, structure_entry, structure_schema};
use function Flow\Types\DSL\{type_integer, type_string, type_structure};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\{JsonDefinition, StructureDefinition};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class StructureDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases() : \Generator
    {
        yield 'same type and name' => [
            structure_schema('data', type_structure(['name' => type_string(), 'age' => type_integer()])),
            structure_schema('data', type_structure(['name' => type_string(), 'age' => type_integer()])),
            true,
        ];

        yield 'same type different name' => [
            structure_schema('data', type_structure(['name' => type_string()])),
            structure_schema('other', type_structure(['name' => type_string()])),
            false,
        ];

        yield 'not nullable with nullable' => [
            structure_schema('data', type_structure(['name' => type_string()]), false),
            structure_schema('data', type_structure(['name' => type_string()]), true),
            false,
        ];

        yield 'nullable with not nullable' => [
            structure_schema('data', type_structure(['name' => type_string()]), true),
            structure_schema('data', type_structure(['name' => type_string()]), false),
            true,
        ];

        yield 'different element types' => [
            structure_schema('data', type_structure(['name' => type_string()])),
            structure_schema('data', type_structure(['name' => type_integer()])),
            false,
        ];

        yield 'different element count' => [
            structure_schema('data', type_structure(['name' => type_string()])),
            structure_schema('data', type_structure(['name' => type_string(), 'age' => type_integer()])),
            false,
        ];
    }

    public static function provideMergeCases() : \Generator
    {
        yield 'same type' => [
            structure_schema('data', type_structure(['name' => type_string()])),
            structure_schema('data', type_structure(['name' => type_string()])),
            structure_schema('data', type_structure(['name' => type_string()])),
        ];

        yield 'makes nullable when other is nullable' => [
            structure_schema('data', type_structure(['name' => type_string()]), false),
            structure_schema('data', type_structure(['name' => type_string()]), true),
            structure_schema('data', type_structure(['name' => type_string()]), true),
        ];

        yield 'with string produces string' => [
            structure_schema('col', type_structure(['name' => type_string()])),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public static function provideMergeWithExpectedTypeCases() : \Generator
    {
        yield 'different structure type produces json' => [
            structure_schema('col', type_structure(['name' => type_string()])),
            structure_schema('col', type_structure(['age' => type_integer()])),
            JsonDefinition::class,
        ];
    }

    public function test_add_metadata() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        $withMeta = $def->addMetadata('key', 'value');

        self::assertTrue($withMeta->metadata()->has('key'));
        self::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        self::assertFalse($def->matches(structure_entry('other', ['name' => 'John'], type_structure(['name' => type_string()]))));
    }

    public function test_does_not_match_entry_with_different_type() : void
    {
        $def = structure_schema('col', type_structure(['name' => type_string()]));

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
        $def = structure_schema('data', type_structure(['name' => type_string()]), false, Metadata::with('key', 'value1'));
        $other = structure_schema('data', type_structure(['name' => type_string()]), false, Metadata::with('key', 'value2'));

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]), true);
        $other = structure_schema('data', type_structure(['name' => type_string()]), false);

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));
        $other = string_schema('data');

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]), true, Metadata::with('key', 'value'));
        $other = structure_schema('data', type_structure(['name' => type_string()]), true, Metadata::with('key', 'value'));

        self::assertTrue($def->isSame($other));
    }

    public function test_make_nullable() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]), false);

        $nullable = $def->makeNullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        self::assertTrue($def->matches(structure_entry('data', ['name' => 'John'], type_structure(['name' => type_string()]))));
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
        $def1 = structure_schema('col', type_structure(['name' => type_string()]), true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = structure_schema('col', type_structure(['name' => type_string()]), true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        self::assertInstanceOf(StructureDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null() : void
    {
        $nullDef = structure_schema('col', type_structure(['name' => type_string()]), true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = structure_schema('col', type_structure(['name' => type_string()]), false);

        $merged = $nullDef->merge($def);

        self::assertInstanceOf(StructureDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type() : void
    {
        $def = structure_schema('col', type_structure(['name' => type_string()]), false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        self::assertInstanceOf(StructureDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, data and other');

        $def->merge(structure_schema('other', type_structure(['name' => type_string()])));
    }

    public function test_merge_with_incompatible_type_throws_exception() : void
    {
        $def = structure_schema('col', type_structure(['name' => type_string()]));

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]), true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        self::assertSame('data', $normalized['ref']);
        self::assertTrue($normalized['nullable']);
        self::assertArrayHasKey('type', $normalized);
        self::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name() : void
    {
        $def = structure_schema('col', type_structure(['name' => type_string()]), true);

        self::assertTrue($def->matches(structure_entry('col', null, type_structure(['name' => type_string()]))));
    }

    public function test_rename() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        $renamed = $def->rename('new_data');

        self::assertSame('new_data', $renamed->entry()->name());
        self::assertSame('data', $def->entry()->name());
    }

    public function test_set_metadata() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        self::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_structure_type() : void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        self::assertStringContainsString('structure', $def->type()->toString());
    }
}
