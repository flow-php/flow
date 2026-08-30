<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type;
use Flow\Types\Type\Comparator;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\ResourceType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\Native\UnionType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_is;
use function Flow\Types\DSL\type_is_any;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;

final class ComparatorTest extends TestCase
{
    public static function type_comparable_data_provider(): Generator
    {
        yield [type_integer(), type_integer()];
        yield [type_json(), type_string()];
        yield [type_integer(), type_float()];
        yield [type_float(), type_integer()];
        yield [type_float(), type_float()];

        yield [type_integer(), type_optional(type_integer())];
        yield [type_float(), type_optional(type_float())];

        yield [type_union(type_integer(), type_null()), type_integer()];
        yield [type_union(type_integer(), type_null()), type_float()];
        yield [type_integer(), type_string()];

        yield [type_array(), type_array()];
        yield [type_array(), type_list(type_string())];
        yield [type_array(), type_map(type_string(), type_integer())];
        yield [type_array(), type_structure(['id' => type_integer()])];
        yield [type_list(type_string()), type_array()];
        yield [type_map(type_string(), type_integer()), type_array()];
        yield [type_structure(['id' => type_integer()]), type_array()];

        yield [type_empty_array(), type_empty_array()];
        yield [type_empty_array(), type_array()];
        yield [type_array(), type_empty_array()];
        yield [type_empty_array(), type_list(type_string())];
        yield [type_empty_array(), type_map(type_string(), type_integer())];
        yield [type_empty_array(), type_structure(['id' => type_integer()])];
        yield [type_list(type_string()), type_empty_array()];
        yield [type_map(type_string(), type_integer()), type_empty_array()];
        yield [type_structure(['id' => type_integer()]), type_empty_array()];
    }

    public static function type_comparison_data_provider(): Generator
    {
        yield [type_integer(), type_float(), false];
        yield [type_integer(), type_string(), false];
        yield [type_null(), type_optional(type_boolean()), false];
        yield [type_integer(), type_integer(), true];
        yield [type_float(), type_float(), true];
        yield [type_string(), type_string(), true];
        yield [type_optional(type_integer()), type_optional(type_integer()), true];
        yield [type_optional(type_integer()), type_optional(type_float()), false];
        yield [type_map(type_string(), type_integer()), type_map(type_string(), type_integer()), true];
        yield [type_map(type_string(), type_integer()), type_map(type_integer(), type_integer()), false];
        yield [type_map(type_string(), type_integer()), type_map(type_string(), type_float()), false];
        yield [type_list(type_string()), type_list(type_string()), true];
        yield [type_list(type_string()), type_list(type_integer()), false];
        yield [type_list(type_integer()), type_list(type_optional(type_integer())), false];
        yield [
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            true,
        ];
        yield [
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            type_structure(['id' => type_integer(), 'name' => type_optional(type_string())]),
            false,
        ];
        yield [
            type_structure(['name' => type_string()]),
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            false,
        ];
        yield [
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            type_structure(['name' => type_string()]),
            false,
        ];
        yield [
            type_structure(['id' => type_integer(), 'name' => type_string()]),
            type_structure(['id' => type_integer(), 'name' => type_string(), 'active' => type_boolean()]),
            false,
        ];
        yield [
            type_structure(['a' => type_integer()]),
            type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)]),
            false,
        ];
        yield [
            type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)]),
            type_structure(['a' => type_integer()]),
            false,
        ];
        yield [
            type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)]),
            type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)]),
            true,
        ];
        yield [
            type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)]),
            type_structure(['a' => type_integer(), 'b' => structure_element('b', type_integer(), optional: true)]),
            false,
        ];
        yield [
            type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)]),
            type_structure(['a' => type_integer(), 'c' => structure_element('c', type_string(), optional: true)]),
            false,
        ];
        yield [
            type_structure(['a' => type_integer()], false),
            type_structure(['a' => type_integer()], true),
            false,
        ];
        yield [
            type_structure(['a' => type_integer()], true),
            type_structure(['a' => type_integer()], true),
            true,
        ];
        yield [
            type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)], true),
            type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)], true),
            true,
        ];
        yield 'same fields, different order' => [
            type_structure(['a' => type_integer(), 'b' => type_string()]),
            type_structure(['b' => type_string(), 'a' => type_integer()]),
            false,
        ];
        yield 'same fields, different order, nested in list' => [
            type_list(type_structure(['a' => type_integer(), 'b' => type_string()])),
            type_list(type_structure(['b' => type_string(), 'a' => type_integer()])),
            false,
        ];
        yield 'interleaved optional equals required-first only when order matches' => [
            type_structure(['z' => type_integer(), 'a' => structure_element('a', type_integer(), optional: true)]),
            type_structure(['a' => structure_element('a', type_integer(), optional: true), 'z' => type_integer()]),
            false,
        ];
    }

    public function test_comparable_follows_declared_field_order(): void
    {
        static::assertFalse((new Comparator())->comparable(
            type_structure(['a' => type_integer(), 'b' => type_string()]),
            type_structure(['b' => type_string(), 'a' => type_integer()]),
        ));
        static::assertTrue((new Comparator())->comparable(
            type_structure(['a' => type_integer(), 'b' => type_string()]),
            type_structure(['a' => type_integer(), 'b' => type_string()]),
        ));
        static::assertTrue((new Comparator())->comparable(type_integer(), type_string()));
    }

    public static function type_not_comparable_data_provider(): Generator
    {
        yield [type_integer(), type_union(type_float(), type_integer())];
        yield [type_integer(), type_boolean()];
        yield [type_array(), type_string()];
        yield [type_array(), type_integer()];
        yield [type_array(), type_boolean()];
        yield [type_string(), type_array()];
        yield [type_empty_array(), type_string()];
        yield [type_empty_array(), type_integer()];
        yield [type_empty_array(), type_boolean()];
        yield [type_string(), type_empty_array()];
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     */
    #[DataProvider('type_comparison_data_provider')]
    public function test_comparing_types(Type $left, Type $right, bool $equals): void
    {
        if ($equals === true) {
            static::assertTrue(type_equals($left, $right));
        } else {
            static::assertFalse(type_equals($left, $right));
        }
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     */
    #[DataProvider('type_comparable_data_provider')]
    public function test_type_comparable(Type $left, Type $right): void
    {
        static::assertTrue((new Comparator())->comparable($left, $right));
    }

    public function test_type_is(): void
    {
        $type = type_string();

        static::assertTrue(type_is($type, StringType::class));
        static::assertFalse(type_is($type, IntegerType::class));
    }

    public function test_type_is_any(): void
    {
        $type = type_string();

        static::assertTrue(type_is_any($type, StringType::class, BooleanType::class));
        static::assertFalse(type_is_any($type, IntegerType::class, FloatType::class));
    }

    public function test_type_is_any_on_optional_type(): void
    {
        $type = type_optional(type_string());

        static::assertTrue(type_is_any($type, StringType::class, BooleanType::class));
        static::assertTrue(type_is_any($type, OptionalType::class, ResourceType::class));
        static::assertFalse(type_is_any($type, IntegerType::class));
    }

    public function test_type_is_any_on_union_type(): void
    {
        $type = type_union(type_integer(), type_boolean(), type_string());

        static::assertTrue(type_is_any($type, UnionType::class, OptionalType::class));
        static::assertTrue(type_is_any($type, IntegerType::class, StringType::class));
        static::assertTrue(type_is_any($type, StringType::class, BooleanType::class));
        static::assertTrue(type_is_any($type, BooleanType::class, IntegerType::class));
        static::assertFalse(type_is_any($type, FloatType::class, MapType::class));
    }

    public function test_type_is_on_optional_type(): void
    {
        $type = type_optional(type_string());

        static::assertTrue(type_is($type, StringType::class));
        static::assertTrue(type_is($type, OptionalType::class));
        static::assertFalse(type_is($type, IntegerType::class));
    }

    public function test_type_is_on_union_type(): void
    {
        $type = type_union(type_integer(), type_boolean(), type_string());

        static::assertTrue(type_is($type, UnionType::class));
        static::assertTrue(type_is($type, IntegerType::class));
        static::assertTrue(type_is($type, StringType::class));
        static::assertTrue(type_is($type, BooleanType::class));
        static::assertFalse(type_is($type, FloatType::class));
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     */
    #[DataProvider('type_not_comparable_data_provider')]
    public function test_type_not_comparable(Type $left, Type $right): void
    {
        static::assertFalse((new Comparator())->comparable($left, $right));
    }
}
