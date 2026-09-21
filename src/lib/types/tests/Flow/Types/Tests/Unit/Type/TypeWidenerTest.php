<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Tests\Unit\Type\Fixtures\SomeEnum;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\TypeWidener;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use stdClass;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_callable;
use function Flow\Types\DSL\type_class_string;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_literal;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_non_empty_string;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_object;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_positive_integer;
use function Flow\Types\DSL\type_resource;
use function Flow\Types\DSL\type_scalar;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

final class TypeWidenerTest extends TestCase
{
    public static function provideWidenCases(): Generator
    {
        yield 'equal types' => [type_string(), type_string(), type_string()];

        // array{} is a container, so it widens with another container to json rather than
        // falling through to the string floor and disagreeing with CommonType one layer up.
        yield 'empty array with a list widens to json' => [
            type_empty_array(),
            type_list(type_string()),
            type_json(),
        ];

        yield 'list with an empty array widens to json' => [
            type_list(type_string()),
            type_empty_array(),
            type_json(),
        ];

        yield 'bottom list with a typed list makes the element optional' => [
            type_list(type_null()),
            type_list(type_string()),
            type_list(type_optional(type_string())),
        ];

        yield 'bottom list with a typed list at depth two' => [
            type_list(type_list(type_null())),
            type_list(type_list(type_string())),
            type_list(type_list(type_optional(type_string()))),
        ];

        yield 'bottom list with a structure widens to json' => [
            type_list(type_null()),
            type_structure(['a' => type_string()]),
            type_json(),
        ];

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
            type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
                'nickname' => structure_element('nickname', type_string(), optional: true),
            ]),
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

        yield 'list against a map widens to json' => [
            type_list(type_string()),
            type_map(type_string(), type_string()),
            type_json(),
        ];

        yield 'array against a structure widens to json' => [
            type_array(),
            type_structure(['id' => type_integer()]),
            type_json(),
        ];

        yield 'json against a list widens to json' => [
            type_json(),
            type_list(type_string()),
            type_json(),
        ];

        yield 'array against a scalar widens to string' => [
            type_array(),
            type_integer(),
            type_string(),
        ];
    }

    /**
     * Every concrete non-null, non-optional type the DSL builds - a superset of any inference candidate set.
     *
     * @return Generator<string, array{Type<mixed>}>
     */
    public static function provideNonNullTypes(): Generator
    {
        yield 'array' => [type_array()];
        yield 'boolean' => [type_boolean()];
        yield 'callable' => [type_callable()];
        yield 'class string' => [type_class_string()];
        yield 'date' => [type_date()];
        yield 'datetime' => [type_datetime()];
        yield 'empty array' => [type_empty_array()];
        yield 'enum' => [type_enum(SomeEnum::class)];
        yield 'float' => [type_float()];
        yield 'html' => [type_html()];
        yield 'html element' => [type_html_element()];
        yield 'instance of' => [type_instance_of(stdClass::class)];
        yield 'integer' => [type_integer()];
        yield 'json' => [type_json()];
        yield 'list' => [type_list(type_integer())];
        yield 'literal' => [type_literal('x')];
        yield 'map' => [type_map(type_string(), type_integer())];
        yield 'non empty string' => [type_non_empty_string()];
        yield 'numeric string' => [type_numeric_string()];
        yield 'object' => [type_object()];
        yield 'positive integer' => [type_positive_integer()];
        yield 'resource' => [type_resource()];
        yield 'scalar' => [type_scalar()];
        yield 'string' => [type_string()];
        yield 'structure' => [type_structure(['a' => type_integer()])];
        yield 'time' => [type_time()];
        yield 'time zone' => [type_time_zone()];
        yield 'uuid' => [type_uuid()];
        yield 'xml' => [type_xml()];
        yield 'xml element' => [type_xml_element()];
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
            type_structure([
                'id' => type_integer(),
                'nickname' => structure_element('nickname', type_string(), optional: true),
            ]),
        ];

        yield 'key missing on the left becomes optional' => [
            type_structure(['id' => type_integer()]),
            type_structure(['id' => type_integer(), 'nickname' => type_string()]),
            type_structure([
                'id' => type_integer(),
                'nickname' => structure_element('nickname', type_string(), optional: true),
            ]),
        ];

        yield 'fully disjoint keys become all optional' => [
            type_structure(['name' => type_string()]),
            type_structure(['age' => type_integer()]),
            type_structure([
                'name' => structure_element('name', type_string(), optional: true),
                'age' => structure_element('age', type_integer(), optional: true),
            ]),
        ];

        yield 'optional on the left stays optional' => [
            type_structure([
                'id' => type_integer(),
                'nickname' => structure_element('nickname', type_string(), optional: true),
            ]),
            type_structure(['id' => type_integer(), 'nickname' => type_string()]),
            type_structure([
                'id' => type_integer(),
                'nickname' => structure_element('nickname', type_string(), optional: true),
            ]),
        ];

        yield 'optional on the right stays optional' => [
            type_structure(['id' => type_integer(), 'nickname' => type_string()]),
            type_structure([
                'id' => type_integer(),
                'nickname' => structure_element('nickname', type_string(), optional: true),
            ]),
            type_structure([
                'id' => type_integer(),
                'nickname' => structure_element('nickname', type_string(), optional: true),
            ]),
        ];

        yield 'optional on both sides stays optional' => [
            type_structure([
                'id' => type_integer(),
                'nickname' => structure_element('nickname', type_string(), optional: true),
            ]),
            type_structure([
                'id' => type_integer(),
                'nickname' => structure_element('nickname', type_string(), optional: true),
            ]),
            type_structure([
                'id' => type_integer(),
                'nickname' => structure_element('nickname', type_string(), optional: true),
            ]),
        ];

        yield 'null element promotes the other side to optional' => [
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            type_structure(['id' => type_integer(), 'name' => type_null()]),
            type_structure(['id' => type_integer(), 'name' => type_optional(type_string())]),
        ];

        yield 'allow extra on the left wins' => [
            type_structure(['id' => type_integer()], true),
            type_structure(['id' => type_integer()], false),
            type_structure(['id' => type_integer()], true),
        ];

        yield 'allow extra on the right wins' => [
            type_structure(['id' => type_integer()], false),
            type_structure(['id' => type_integer()], true),
            type_structure(['id' => type_integer()], true),
        ];

        yield 'allow extra false on both sides stays false' => [
            type_structure(['id' => type_integer()], false),
            type_structure(['id' => type_integer()], false),
            type_structure(['id' => type_integer()], false),
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
            type_structure(['user' => type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
            ])]),
        ];

        yield 'conflict in a one sided key is impossible, it stays optional' => [
            type_structure(['name' => type_string()]),
            type_structure(['name' => type_string(), 'age' => type_integer()]),
            type_structure([
                'name' => type_string(),
                'age' => structure_element('age', type_integer(), optional: true),
            ]),
        ];
    }

    /**
     * @param Type<mixed> $candidate
     */
    #[DataProvider('provideNonNullTypes')]
    public function test_widening_optional_string_with_any_non_null_candidate_type_yields_optional_string(Type $candidate): void
    {
        static::assertEquals(
            type_optional(type_string()),
            (new TypeWidener())->widen(type_optional(type_string()), $candidate),
        );
    }

    /**
     * @param Type<mixed> $candidate
     */
    #[DataProvider('provideNonNullTypes')]
    public function test_widening_string_with_any_non_null_candidate_type_yields_string(Type $candidate): void
    {
        static::assertEquals(type_string(), (new TypeWidener())->widen(type_string(), $candidate));
    }

    public function test_widening_string_with_null_yields_optional_string(): void
    {
        static::assertEquals(type_optional(type_string()), (new TypeWidener())->widen(type_string(), type_null()));
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     * @param Type<mixed> $expected
     */
    #[DataProvider('provideWidenCases')]
    public function test_widening_types(Type $left, Type $right, Type $expected): void
    {
        static::assertEquals($expected, (new TypeWidener())->widen($left, $right));
    }

    /**
     * @param StructureType<array<array-key, mixed>> $left
     * @param StructureType<array<array-key, mixed>> $right
     * @param StructureType<array<array-key, mixed>> $expected
     */
    #[DataProvider('provideStructureCases')]
    public function test_widening_structures(StructureType $left, StructureType $right, StructureType $expected): void
    {
        static::assertEquals($expected, (new TypeWidener())->widenStructures($left, $right));
    }

    public function test_widening_lists_directly(): void
    {
        static::assertEquals(
            type_list(type_float()),
            (new TypeWidener())->widenLists(type_list(type_integer()), type_list(type_float())),
        );
    }

    public function test_widening_maps_directly(): void
    {
        static::assertEquals(
            type_map(type_string(), type_float()),
            (new TypeWidener())->widenMaps(
                type_map(type_string(), type_integer()),
                type_map(type_string(), type_float()),
            ),
        );
    }

    #[TestWith(['widen'])]
    #[TestWith(['widenLists'])]
    #[TestWith(['widenMaps'])]
    #[TestWith(['widenStructures'])]
    public function test_widening_takes_only_the_two_operands(string $method): void
    {
        static::assertSame(2, (new ReflectionMethod(TypeWidener::class, $method))->getNumberOfParameters());
    }

    public function test_structure_key_order_follows_left_then_right(): void
    {
        static::assertSame(
            'structure{b: string, a?: string, c?: string}',
            (new TypeWidener())
                ->widen(
                    type_structure(['b' => type_string(), 'a' => type_string()]),
                    type_structure(['c' => type_string(), 'b' => type_string()]),
                )
                ->toString(),
        );
    }

    public function test_structure_widening_keeps_left_declared_order_and_appends_right_only_names(): void
    {
        static::assertSame(
            'structure{a?: integer, b: integer}',
            (new TypeWidener())
                ->widen(
                    type_structure(['a' => type_integer(), 'b' => type_integer()]),
                    type_structure(['b' => type_integer()]),
                )
                ->toString(),
        );

        static::assertSame(
            'structure{b: integer, a?: integer}',
            (new TypeWidener())
                ->widen(
                    type_structure(['b' => type_integer()]),
                    type_structure(['a' => type_integer(), 'b' => type_integer()]),
                )
                ->toString(),
        );
    }
}
