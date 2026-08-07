<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Schema\Definition\TypeMerge;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class TypeMergeTest extends FlowTestCase
{
    public static function provideMergeCases(): Generator
    {
        yield 'equal types' => [type_string(), type_string(), type_string()];

        yield 'equal null types' => [type_null(), type_null(), type_null()];

        yield 'null on the left' => [type_null(), type_string(), type_optional(type_string())];

        yield 'null on the right' => [type_string(), type_null(), type_optional(type_string())];

        yield 'null on the left, optional on the right does not double wrap' => [
            type_null(),
            type_optional(type_string()),
            type_optional(type_string()),
        ];

        yield 'optional on the left, null on the right does not double wrap' => [
            type_optional(type_string()),
            type_null(),
            type_optional(type_string()),
        ];

        yield 'optional on the left and its base on the right' => [
            type_optional(type_string()),
            type_string(),
            type_optional(type_string()),
        ];

        yield 'base on the left and its optional on the right' => [
            type_string(),
            type_optional(type_string()),
            type_optional(type_string()),
        ];

        yield 'integer and float promote to float' => [type_integer(), type_float(), type_float()];

        yield 'float and integer promote to float' => [type_float(), type_integer(), type_float()];

        yield 'nullable integer and float promote to nullable float' => [
            type_optional(type_integer()),
            type_float(),
            type_optional(type_float()),
        ];

        yield 'date and datetime promote to datetime' => [type_date(), type_datetime(), type_datetime()];

        yield 'datetime and date promote to datetime' => [type_datetime(), type_date(), type_datetime()];

        yield 'integer and string do not promote' => [type_integer(), type_string(), type_string()];

        yield 'date and string do not promote' => [type_date(), type_string(), type_string()];

        yield 'conflicting scalars widen to string' => [type_string(), type_boolean(), type_string()];

        yield 'optional of conflicting scalars widens and stays optional' => [
            type_optional(type_string()),
            type_optional(type_boolean()),
            type_optional(type_string()),
        ];

        yield 'optional against an unrelated base type' => [
            type_optional(type_string()),
            type_boolean(),
            type_optional(type_string()),
        ];

        yield 'nested structures merge recursively' => [
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            type_structure(['id' => type_integer(), 'nickname' => type_string()]),
            type_structure(['id' => type_integer()], ['name' => type_string(), 'nickname' => type_string()]),
        ];

        yield 'nested structures widen only the conflicting element' => [
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            type_structure(['id' => type_integer(), 'name' => type_boolean()]),
            type_structure(['id' => type_integer(), 'name' => type_string()]),
        ];

        yield 'lists recurse on their element type' => [
            type_list(type_integer()),
            type_list(type_boolean()),
            type_list(type_string()),
        ];

        yield 'lists promote a numeric element' => [
            type_list(type_integer()),
            type_list(type_float()),
            type_list(type_float()),
        ];

        yield 'lists with a null element become optional' => [
            type_list(type_null()),
            type_list(type_string()),
            type_list(type_optional(type_string())),
        ];

        yield 'maps recurse on their value type' => [
            type_map(type_string(), type_integer()),
            type_map(type_string(), type_boolean()),
            type_map(type_string(), type_string()),
        ];

        yield 'maps promote a numeric value' => [
            type_map(type_string(), type_integer()),
            type_map(type_string(), type_float()),
            type_map(type_string(), type_float()),
        ];

        yield 'maps widen a differing key to string' => [
            type_map(type_string(), type_integer()),
            type_map(type_integer(), type_integer()),
            type_map(type_string(), type_integer()),
        ];

        yield 'structure against a scalar widens to string' => [
            type_structure(['id' => type_integer()]),
            type_integer(),
            type_string(),
        ];

        yield 'list against a map widens to string' => [
            type_list(type_string()),
            type_map(type_string(), type_string()),
            type_string(),
        ];
    }

    public static function provideStructureCases(): Generator
    {
        yield 'identical structures are unchanged' => [
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            type_structure(['id' => type_integer(), 'name' => type_string()]),
        ];

        yield 'key missing on the right becomes optional' => [
            type_structure(['id' => type_integer(), 'nickname' => type_string()]),
            type_structure(['id' => type_integer()]),
            type_structure(['id' => type_integer()], ['nickname' => type_string()]),
        ];

        yield 'key missing on the left becomes optional' => [
            type_structure(['id' => type_integer()]),
            type_structure(['id' => type_integer(), 'nickname' => type_string()]),
            type_structure(['id' => type_integer()], ['nickname' => type_string()]),
        ];

        yield 'fully disjoint keys become all optional' => [
            type_structure(['name' => type_string()]),
            type_structure(['age' => type_integer()]),
            type_structure([], ['name' => type_string(), 'age' => type_integer()]),
        ];

        yield 'optional on the left stays optional' => [
            type_structure(['id' => type_integer()], ['nickname' => type_string()]),
            type_structure(['id' => type_integer(), 'nickname' => type_string()]),
            type_structure(['id' => type_integer()], ['nickname' => type_string()]),
        ];

        yield 'optional on the right stays optional' => [
            type_structure(['id' => type_integer(), 'nickname' => type_string()]),
            type_structure(['id' => type_integer()], ['nickname' => type_string()]),
            type_structure(['id' => type_integer()], ['nickname' => type_string()]),
        ];

        yield 'optional on both sides stays optional' => [
            type_structure(['id' => type_integer()], ['nickname' => type_string()]),
            type_structure(['id' => type_integer()], ['nickname' => type_string()]),
            type_structure(['id' => type_integer()], ['nickname' => type_string()]),
        ];

        yield 'null element promotes the other side to optional' => [
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            type_structure(['id' => type_integer(), 'name' => type_null()]),
            type_structure(['id' => type_integer(), 'name' => type_optional(type_string())]),
        ];

        yield 'allow extra on the left wins' => [
            type_structure(['id' => type_integer()], [], true),
            type_structure(['id' => type_integer()], [], false),
            type_structure(['id' => type_integer()], [], true),
        ];

        yield 'allow extra on the right wins' => [
            type_structure(['id' => type_integer()], [], false),
            type_structure(['id' => type_integer()], [], true),
            type_structure(['id' => type_integer()], [], true),
        ];

        yield 'allow extra false on both sides stays false' => [
            type_structure(['id' => type_integer()], [], false),
            type_structure(['id' => type_integer()], [], false),
            type_structure(['id' => type_integer()], [], false),
        ];

        yield 'nested structures merge recursively when equal' => [
            type_structure(['user' => type_structure(['id' => type_integer()])]),
            type_structure(['user' => type_structure(['id' => type_integer()])]),
            type_structure(['user' => type_structure(['id' => type_integer()])]),
        ];

        yield 'a conflicting element widens without discarding the others' => [
            type_structure(['id' => type_integer(), 'email' => type_string(), 'name' => type_string()]),
            type_structure(['id' => type_integer(), 'email' => type_string(), 'name' => type_boolean()]),
            type_structure(['id' => type_integer(), 'email' => type_string(), 'name' => type_string()]),
        ];

        yield 'nested structures merge instead of collapsing the parent' => [
            type_structure(['user' => type_structure(['id' => type_integer(), 'name' => type_string()])]),
            type_structure(['user' => type_structure(['id' => type_integer()])]),
            type_structure(['user' => type_structure(['id' => type_integer()], ['name' => type_string()])]),
        ];

        yield 'conflict in a one sided key is impossible, it stays optional' => [
            type_structure(['name' => type_string()]),
            type_structure(['name' => type_string(), 'age' => type_integer()]),
            type_structure(['name' => type_string()], ['age' => type_integer()]),
        ];
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     * @param Type<mixed> $expected
     */
    #[DataProvider('provideMergeCases')]
    public function test_merging_types(Type $left, Type $right, Type $expected): void
    {
        static::assertEquals($expected, (new TypeMerge())->merge($left, $right));
    }

    /**
     * @param StructureType<array<array-key, mixed>> $left
     * @param StructureType<array<array-key, mixed>> $right
     * @param StructureType<array<array-key, mixed>> $expected
     */
    #[DataProvider('provideStructureCases')]
    public function test_merging_structures(StructureType $left, StructureType $right, StructureType $expected): void
    {
        static::assertEquals($expected, (new TypeMerge())->mergeStructures($left, $right));
    }

    public function test_merging_lists_directly(): void
    {
        static::assertEquals(
            type_list(type_float()),
            (new TypeMerge())->mergeLists(type_list(type_integer()), type_list(type_float())),
        );
    }

    public function test_merging_maps_directly(): void
    {
        static::assertEquals(
            type_map(type_string(), type_float()),
            (new TypeMerge())->mergeMaps(
                type_map(type_string(), type_integer()),
                type_map(type_string(), type_float()),
            ),
        );
    }

    public function test_structure_key_order_follows_left_then_right(): void
    {
        static::assertSame(
            'structure{b: string, a?: string, c?: string}',
            (new TypeMerge())
                ->merge(
                    type_structure(['b' => type_string(), 'a' => type_string()]),
                    type_structure(['c' => type_string(), 'b' => type_string()]),
                )
                ->toString(),
        );
    }
}
