<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Definition\StructureDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_entry;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class StructureDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
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

    public static function provideMergeCases(): Generator
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

    public static function provideMergeWithExpectedTypeCases(): Generator
    {
        yield 'different structure type produces json' => [
            structure_schema('col', type_structure(['name' => type_string()])),
            structure_schema('col', type_structure(['age' => type_integer()])),
            JsonDefinition::class,
        ];
    }

    public function test_add_metadata(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        static::assertFalse($def->matches(structure_entry('other', ['name' => 'John'], type_structure([
            'name' => type_string(),
        ]))));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = structure_schema('col', type_structure(['name' => type_string()]));

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_entry_class(): void
    {
        static::assertSame(
            StructureEntry::class,
            structure_schema('data', type_structure(['name' => type_string()]))->entryClass(),
        );
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
        $def = structure_schema(
            'data',
            type_structure(['name' => type_string()]),
            false,
            Metadata::with('key', 'value1'),
        );
        $other = structure_schema(
            'data',
            type_structure(['name' => type_string()]),
            false,
            Metadata::with('key', 'value2'),
        );

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]), true);
        $other = structure_schema('data', type_structure(['name' => type_string()]), false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));
        $other = string_schema('data');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = structure_schema(
            'data',
            type_structure(['name' => type_string()]),
            true,
            Metadata::with('key', 'value'),
        );
        $other = structure_schema(
            'data',
            type_structure(['name' => type_string()]),
            true,
            Metadata::with('key', 'value'),
        );

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]), false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        static::assertTrue($def->matches(structure_entry('data', ['name' => 'John'], type_structure([
            'name' => type_string(),
        ]))));
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
        $def1 = structure_schema('col', type_structure(['name' => type_string()]), true, Metadata::fromArray([
            Metadata::FROM_NULL => true,
        ]));
        $def2 = structure_schema('col', type_structure(['name' => type_string()]), true, Metadata::fromArray([
            Metadata::FROM_NULL => true,
        ]));

        $merged = $def1->merge($def2);

        static::assertInstanceOf(StructureDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
        static::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null(): void
    {
        $nullDef = structure_schema('col', type_structure(['name' => type_string()]), true, Metadata::fromArray([
            Metadata::FROM_NULL => true,
        ]));
        $def = structure_schema('col', type_structure(['name' => type_string()]), false);

        $merged = $nullDef->merge($def);

        static::assertInstanceOf(StructureDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
        static::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type(): void
    {
        $def = structure_schema('col', type_structure(['name' => type_string()]), false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        static::assertInstanceOf(StructureDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, data and other');

        $def->merge(structure_schema('other', type_structure(['name' => type_string()])));
    }

    public function test_merge_with_incompatible_type_throws_exception(): void
    {
        $def = structure_schema('col', type_structure(['name' => type_string()]));

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize(): void
    {
        $def = structure_schema(
            'data',
            type_structure(['name' => type_string()]),
            true,
            Metadata::with('key', 'value'),
        );

        $normalized = $def->normalize();

        static::assertSame('data', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name(): void
    {
        $def = structure_schema('col', type_structure(['name' => type_string()]), true);

        static::assertTrue($def->matches(structure_entry('col', null, type_structure(['name' => type_string()]))));
    }

    public function test_rename(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        $renamed = $def->rename('new_data');

        static::assertSame('new_data', $renamed->entry()->name());
        static::assertSame('data', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_structure_type(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        static::assertStringContainsString('structure', $def->type()->toString());
    }
}
