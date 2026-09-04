<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Inference;

use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\DateDefinition;
use Flow\ETL\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Schema\Definition\EnumDefinition;
use Flow\ETL\Schema\Definition\FloatDefinition;
use Flow\ETL\Schema\Definition\HTMLDefinition;
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Definition\ListDefinition;
use Flow\ETL\Schema\Definition\MapDefinition;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Definition\StructureDefinition;
use Flow\ETL\Schema\Definition\TimeDefinition;
use Flow\ETL\Schema\Definition\TimeZoneDefinition;
use Flow\ETL\Schema\Definition\UuidDefinition;
use Flow\ETL\Schema\Definition\XMLDefinition;
use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\ETL\Schema\Inference\TypeFloor;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Flow\Types\Type\Logical\InstanceOfType;
use Flow\Types\Type\Logical\StructureType;
use PHPUnit\Framework\Attributes\DataProvider;
use stdClass;

use function array_map;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

final class TypeFloorTest extends FlowTestCase
{
    /**
     * Admits every leaf the two narrowers can produce, so the table below isolates the floor's own rules from
     * the candidate filter.
     */
    public static function everyLeaf(): InferredTypes
    {
        return new InferredTypes(
            type_json(),
            type_uuid(),
            type_html(),
            type_xml(),
            type_float(),
            type_integer(),
            type_datetime(),
            type_date(),
            type_time(),
            type_boolean(),
            type_time_zone(),
            type_enum(BackedStringEnum::class),
            type_string(),
        );
    }

    /**
     * Every non-container leaf becomes string; containers keep their shape.
     *
     * @return array<string, array{Type<mixed>, Type<mixed>, class-string}>
     */
    public static function allStringsTable(): array
    {
        return [
            'null' => [type_null(), type_string(), StringDefinition::class],
            '?null' => [type_optional(type_null()), type_optional(type_string()), StringDefinition::class],
            'integer' => [type_integer(), type_string(), StringDefinition::class],
            '?integer' => [type_optional(type_integer()), type_optional(type_string()), StringDefinition::class],
            'float' => [type_float(), type_string(), StringDefinition::class],
            'boolean' => [type_boolean(), type_string(), StringDefinition::class],
            'datetime' => [type_datetime(), type_string(), StringDefinition::class],
            'date' => [type_date(), type_string(), StringDefinition::class],
            'time' => [type_time(), type_string(), StringDefinition::class],
            'uuid' => [type_uuid(), type_string(), StringDefinition::class],
            'json' => [type_json(), type_string(), StringDefinition::class],
            'timezone' => [type_time_zone(), type_string(), StringDefinition::class],
            'xml' => [type_xml(), type_string(), StringDefinition::class],
            'html' => [type_html(), type_string(), StringDefinition::class],
            'enum' => [type_enum(BackedStringEnum::class), type_string(), StringDefinition::class],
            'list<null>' => [type_list(type_null()), type_json(), JsonDefinition::class],
            '?list<null>' => [type_optional(type_list(type_null())), type_optional(type_json()), JsonDefinition::class],
            'list<list<null>>' => [type_list(type_list(type_null())), type_list(type_json()), ListDefinition::class],
            'list<?integer>' => [
                type_list(type_optional(type_integer())),
                type_list(type_optional(type_string())),
                ListDefinition::class,
            ],
            'list<array<mixed>>' => [type_list(type_array()), type_list(type_string()), ListDefinition::class],
            'structure{a: null}' => [
                type_structure(['a' => type_null()]),
                type_structure(['a' => type_string()]),
                StructureDefinition::class,
            ],
            'structure{a: list<null>}' => [
                type_structure(['a' => type_list(type_null())]),
                type_structure(['a' => type_json()]),
                StructureDefinition::class,
            ],
            'structure{a: ?integer, b: string}' => [
                type_structure(['a' => type_optional(type_integer()), 'b' => type_string()]),
                type_structure(['a' => type_optional(type_string()), 'b' => type_string()]),
                StructureDefinition::class,
            ],
            'map<string, null>' => [
                type_map(type_string(), type_null()),
                type_map(type_string(), type_string()),
                MapDefinition::class,
            ],
            'map<integer, string>' => [
                type_map(type_integer(), type_string()),
                type_map(type_integer(), type_string()),
                MapDefinition::class,
            ],
            'array<mixed>' => [type_array(), type_string(), StringDefinition::class],
            'array{}' => [type_empty_array(), type_string(), StringDefinition::class],
            'instance_of' => [new InstanceOfType(stdClass::class), type_string(), StringDefinition::class],
            'mixed' => [type_mixed(), type_string(), StringDefinition::class],
            'union' => [type_union(type_integer(), type_string()), type_string(), StringDefinition::class],
        ];
    }

    /**
     * Only the leaves no Definition or cast accepts move.
     *
     * @return array<string, array{Type<mixed>, Type<mixed>, class-string}>
     */
    public static function scalarsKeptTable(): array
    {
        return [
            'null' => [type_null(), type_string(), StringDefinition::class],
            '?null' => [type_optional(type_null()), type_optional(type_string()), StringDefinition::class],
            'integer' => [type_integer(), type_integer(), IntegerDefinition::class],
            '?integer' => [type_optional(type_integer()), type_optional(type_integer()), IntegerDefinition::class],
            'float' => [type_float(), type_float(), FloatDefinition::class],
            'boolean' => [type_boolean(), type_boolean(), BooleanDefinition::class],
            'datetime' => [type_datetime(), type_datetime(), DateTimeDefinition::class],
            'date' => [type_date(), type_date(), DateDefinition::class],
            'time' => [type_time(), type_time(), TimeDefinition::class],
            'uuid' => [type_uuid(), type_uuid(), UuidDefinition::class],
            'json' => [type_json(), type_json(), JsonDefinition::class],
            'timezone' => [type_time_zone(), type_time_zone(), TimeZoneDefinition::class],
            'xml' => [type_xml(), type_xml(), XMLDefinition::class],
            'html' => [type_html(), type_html(), HTMLDefinition::class],
            'enum' => [
                type_enum(BackedStringEnum::class),
                type_enum(BackedStringEnum::class),
                EnumDefinition::class,
            ],
            'list<null>' => [type_list(type_null()), type_json(), JsonDefinition::class],
            '?list<null>' => [type_optional(type_list(type_null())), type_optional(type_json()), JsonDefinition::class],
            'list<list<null>>' => [type_list(type_list(type_null())), type_list(type_json()), ListDefinition::class],
            'list<?integer>' => [
                type_list(type_optional(type_integer())),
                type_list(type_optional(type_integer())),
                ListDefinition::class,
            ],
            'list<array<mixed>>' => [type_list(type_array()), type_list(type_json()), ListDefinition::class],
            'structure{a: null}' => [
                type_structure(['a' => type_null()]),
                type_structure(['a' => type_string()]),
                StructureDefinition::class,
            ],
            'structure{a: list<null>}' => [
                type_structure(['a' => type_list(type_null())]),
                type_structure(['a' => type_json()]),
                StructureDefinition::class,
            ],
            'structure{a: ?integer, b: string}' => [
                type_structure(['a' => type_optional(type_integer()), 'b' => type_string()]),
                type_structure(['a' => type_optional(type_integer()), 'b' => type_string()]),
                StructureDefinition::class,
            ],
            'map<string, null>' => [
                type_map(type_string(), type_null()),
                type_map(type_string(), type_string()),
                MapDefinition::class,
            ],
            'map<integer, string>' => [
                type_map(type_integer(), type_string()),
                type_map(type_integer(), type_string()),
                MapDefinition::class,
            ],
            'array<mixed>' => [type_array(), type_json(), JsonDefinition::class],
            'array{}' => [type_empty_array(), type_json(), JsonDefinition::class],
            'instance_of' => [new InstanceOfType(stdClass::class), type_string(), StringDefinition::class],
            'mixed' => [type_mixed(), type_string(), StringDefinition::class],
            'union' => [type_union(type_integer(), type_string()), type_string(), StringDefinition::class],
        ];
    }

    /**
     * @param Type<mixed> $input
     * @param Type<mixed> $expected
     * @param class-string $definition
     */
    #[DataProvider('allStringsTable')]
    public function test_flooring_with_all_strings(Type $input, Type $expected, string $definition): void
    {
        $floored = (new TypeFloor(new InferredTypes(type_string())))->floor($input);

        static::assertTrue(type_equals($expected, $floored), $expected->toString() . ' !== ' . $floored->toString());
        static::assertInstanceOf($definition, definition_from_type('c', $floored, nullable: true));
    }

    /**
     * @param Type<mixed> $input
     * @param Type<mixed> $expected
     * @param class-string $definition
     */
    #[DataProvider('scalarsKeptTable')]
    public function test_flooring_with_scalars_kept(Type $input, Type $expected, string $definition): void
    {
        $floored = (new TypeFloor(self::everyLeaf()))->floor($input);

        static::assertTrue(type_equals($expected, $floored), $expected->toString() . ' !== ' . $floored->toString());
        static::assertInstanceOf($definition, definition_from_type('c', $floored, nullable: true));
    }

    public function test_structure_element_order_and_optional_flags_survive_the_floor(): void
    {
        $floored = (new TypeFloor(new InferredTypes(type_string())))->floor(type_structure([
            'b' => structure_element('b', type_null(), optional: true),
            'a' => type_integer(),
        ], allow_extra: true));

        static::assertInstanceOf(StructureType::class, $floored);
        static::assertSame(['b', 'a'], array_map(static fn($element) => $element->name, $floored->elements()));
        static::assertTrue($floored->elements()[0]->optional);
        static::assertFalse($floored->elements()[1]->optional);
        static::assertTrue($floored->allowsExtra());
    }

    public function test_the_array_to_json_projection_happens_at_depth(): void
    {
        static::assertTrue(type_equals(
            type_list(type_json()),
            (new TypeFloor(self::everyLeaf()))->floor(type_list(type_array())),
        ));
        static::assertTrue(type_equals(type_structure([
            'a' => type_json(),
        ]), (new TypeFloor(self::everyLeaf()))->floor(type_structure(['a' => type_empty_array()]))));
    }
}
