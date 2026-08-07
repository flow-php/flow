<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\StructureDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_entry;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
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

        yield 'declared optional, given present as required' => [
            structure_schema('data', type_structure(['id' => type_integer()], ['nickname' => type_string()])),
            structure_schema('data', type_structure(['id' => type_integer(), 'nickname' => type_string()])),
            true,
        ];

        yield 'declared optional, given present as optional' => [
            structure_schema('data', type_structure(['id' => type_integer()], ['nickname' => type_string()])),
            structure_schema('data', type_structure(['id' => type_integer()], ['nickname' => type_string()])),
            true,
        ];

        yield 'declared optional, given absent' => [
            structure_schema('data', type_structure(['id' => type_integer()], ['nickname' => type_string()])),
            structure_schema('data', type_structure(['id' => type_integer()])),
            true,
        ];

        yield 'declared optional, given present with a different type' => [
            structure_schema('data', type_structure(['id' => type_integer()], ['nickname' => type_string()])),
            structure_schema('data', type_structure(['id' => type_integer(), 'nickname' => type_boolean()])),
            false,
        ];

        yield 'declared required, given optional so it may be absent' => [
            structure_schema('data', type_structure(['id' => type_integer(), 'nickname' => type_string()])),
            structure_schema('data', type_structure(['id' => type_integer()], ['nickname' => type_string()])),
            false,
        ];

        yield 'declared required, given absent' => [
            structure_schema('data', type_structure(['id' => type_integer(), 'nickname' => type_string()])),
            structure_schema('data', type_structure(['id' => type_integer()])),
            false,
        ];

        yield 'allow extra accepts an undeclared key' => [
            structure_schema('data', type_structure(['id' => type_integer()], [], true)),
            structure_schema('data', type_structure(['id' => type_integer(), 'nickname' => type_string()])),
            true,
        ];

        yield 'allow extra accepts an undeclared optional key' => [
            structure_schema('data', type_structure(['id' => type_integer()], [], true)),
            structure_schema('data', type_structure(['id' => type_integer()], ['nickname' => type_string()])),
            true,
        ];

        yield 'without allow extra an undeclared key is rejected' => [
            structure_schema('data', type_structure(['id' => type_integer()], [], false)),
            structure_schema('data', type_structure(['id' => type_integer(), 'nickname' => type_string()])),
            false,
        ];

        yield 'without allow extra an undeclared optional key is rejected' => [
            structure_schema('data', type_structure(['id' => type_integer()], [], false)),
            structure_schema('data', type_structure(['id' => type_integer()], ['nickname' => type_string()])),
            false,
        ];

        yield 'openness is read from the declared side only' => [
            structure_schema('data', type_structure(['id' => type_integer()], [], false)),
            structure_schema('data', type_structure(['id' => type_integer()], [], true)),
            true,
        ];

        yield 'declared optional element accepts a null value' => [
            structure_schema('data', type_structure([
                'id' => type_integer(),
                'nickname' => type_optional(type_string()),
            ])),
            structure_schema('data', type_structure(['id' => type_integer(), 'nickname' => type_null()])),
            true,
        ];

        yield 'declared required element rejects a null value' => [
            structure_schema('data', type_structure(['id' => type_integer(), 'nickname' => type_string()])),
            structure_schema('data', type_structure(['id' => type_integer(), 'nickname' => type_null()])),
            false,
        ];

        yield 'nested structure element compatibility recurses' => [
            structure_schema('data', type_structure([
                'user' => type_structure(['id' => type_integer()], ['nickname' => type_string()]),
            ])),
            structure_schema('data', type_structure([
                'user' => type_structure(['id' => type_integer(), 'nickname' => type_string()]),
            ])),
            true,
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

        yield 'key missing on one side becomes optional' => [
            structure_schema('data', type_structure([
                'id' => type_integer(),
                'email' => type_string(),
                'nickname' => type_string(),
            ])),
            structure_schema('data', type_structure(['id' => type_integer(), 'email' => type_string()])),
            structure_schema('data', type_structure(['id' => type_integer(), 'email' => type_string()], [
                'nickname' => type_string(),
            ])),
        ];

        yield 'null and a type become optional of that type' => [
            structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()])),
            structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_null()])),
            structure_schema('data', type_structure([
                'id' => type_integer(),
                'name' => type_optional(type_string()),
            ])),
        ];

        yield 'null on the left and a type on the right' => [
            structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_null()])),
            structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()])),
            structure_schema('data', type_structure([
                'id' => type_integer(),
                'name' => type_optional(type_string()),
            ])),
        ];

        yield 'null on both sides stays null' => [
            structure_schema('data', type_structure(['name' => type_null()])),
            structure_schema('data', type_structure(['name' => type_null()])),
            structure_schema('data', type_structure(['name' => type_null()])),
        ];

        yield 'null and an already optional type does not double wrap' => [
            structure_schema('data', type_structure(['name' => type_null()])),
            structure_schema('data', type_structure(['name' => type_optional(type_string())])),
            structure_schema('data', type_structure(['name' => type_optional(type_string())])),
        ];

        yield 'a type and its optional collapse to the optional' => [
            structure_schema('data', type_structure(['name' => type_string()])),
            structure_schema('data', type_structure(['name' => type_optional(type_string())])),
            structure_schema('data', type_structure(['name' => type_optional(type_string())])),
        ];

        yield 'optional on the left and a bare type on the right' => [
            structure_schema('data', type_structure(['name' => type_optional(type_string())])),
            structure_schema('data', type_structure(['name' => type_string()])),
            structure_schema('data', type_structure(['name' => type_optional(type_string())])),
        ];

        yield 'optional element on either side stays optional' => [
            structure_schema('data', type_structure(['id' => type_integer()], ['name' => type_string()])),
            structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()])),
            structure_schema('data', type_structure(['id' => type_integer()], ['name' => type_string()])),
        ];

        yield 'allow extra is the union of both sides' => [
            structure_schema('data', type_structure(['id' => type_integer()], [], false)),
            structure_schema('data', type_structure(['id' => type_integer()], [], true)),
            structure_schema('data', type_structure(['id' => type_integer()], [], true)),
        ];

        yield 'allow extra false on both sides stays false' => [
            structure_schema('data', type_structure(['id' => type_integer()], [], false)),
            structure_schema('data', type_structure(['id' => type_integer()], [], false)),
            structure_schema('data', type_structure(['id' => type_integer()], [], false)),
        ];

        yield 'disjoint keys stay a structure, both optional' => [
            structure_schema('col', type_structure(['name' => type_string()])),
            structure_schema('col', type_structure(['age' => type_integer()])),
            structure_schema('col', type_structure([], ['name' => type_string(), 'age' => type_integer()])),
        ];

        yield 'conflicting element widens to string instead of collapsing to json' => [
            structure_schema('col', type_structure(['name' => type_string()])),
            structure_schema('col', type_structure(['name' => type_integer()])),
            structure_schema('col', type_structure(['name' => type_string()])),
        ];

        yield 'one conflicting element does not discard the others' => [
            structure_schema('col', type_structure(['id' => type_integer(), 'name' => type_string()])),
            structure_schema('col', type_structure(['id' => type_integer(), 'name' => type_boolean()])),
            structure_schema('col', type_structure(['id' => type_integer(), 'name' => type_string()])),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
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

    public function test_merge_with_null_definition_keeps_original_type(): void
    {
        $merged = structure_schema('col', type_structure(['name' => type_string()]), false)->merge(null_schema('col'));

        static::assertInstanceOf(StructureDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(structure_schema('col', type_structure(['name' => type_string()]), false));

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
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_structure_type(): void
    {
        $def = structure_schema('data', type_structure(['name' => type_string()]));

        static::assertStringContainsString('structure', $def->type()->toString());
    }
}
