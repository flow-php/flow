<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use function Flow\Types\DSL\{type_boolean,
    type_equals,
    type_float,
    type_integer,
    type_is,
    type_is_any,
    type_json,
    type_list,
    type_map,
    type_null,
    type_optional,
    type_string,
    type_structure,
    type_union};
use Flow\Types\Type;
use Flow\Types\Type\{Comparator};
use Flow\Types\Type\Logical\{MapType, OptionalType};
use Flow\Types\Type\Native\{BooleanType, FloatType, IntegerType, ResourceType, StringType, UnionType};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ComparatorTest extends TestCase
{
    public static function type_comparable_data_provider() : \Generator
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
    }

    public static function type_comparison_data_provider() : \Generator
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
        yield [type_structure(['id' => type_integer(), 'name' => type_string()]), type_structure(['id' => type_integer(), 'name' => type_string()]), true];
        yield [type_structure(['id' => type_integer(), 'name' => type_string()]), type_structure(['id' => type_integer(), 'name' => type_optional(type_string())]), false];
        yield [type_structure(['name' => type_string()]), type_structure(['id' => type_integer(), 'name' => type_string()]), false];
        yield [type_structure(['id' => type_integer(), 'name' => type_string()]), type_structure(['name' => type_string()]), false];
        yield [type_structure(['id' => type_integer(), 'name' => type_string()]), type_structure(['id' => type_integer(), 'name' => type_string(), 'active' => type_boolean()]), false];
    }

    public static function type_not_comparable_data_provider() : \Generator
    {
        yield [type_integer(), type_union(type_float(), type_integer())];
        yield [type_integer(), type_boolean()];
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     */
    #[DataProvider('type_comparison_data_provider')]
    public function test_comparing_types(Type $left, Type $right, bool $equals) : void
    {
        if ($equals === true) {
            self::assertTrue(type_equals($left, $right));
        } else {
            self::assertFalse(type_equals($left, $right));
        }
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     */
    #[DataProvider('type_comparable_data_provider')]
    public function test_type_comparable(Type $left, Type $right) : void
    {
        self::assertTrue((new Comparator())->comparable($left, $right));
    }

    public function test_type_is() : void
    {
        $type = type_string();

        self::assertTrue(type_is($type, StringType::class));
        self::assertFalse(type_is($type, IntegerType::class));
    }

    public function test_type_is_any() : void
    {
        $type = type_string();

        self::assertTrue(type_is_any($type, StringType::class, BooleanType::class));
        self::assertFalse(type_is_any($type, IntegerType::class, FloatType::class));
    }

    public function test_type_is_any_on_optional_type() : void
    {
        $type = type_optional(type_string());

        self::assertTrue(type_is_any($type, StringType::class, BooleanType::class));
        self::assertTrue(type_is_any($type, OptionalType::class, ResourceType::class));
        self::assertFalse(type_is_any($type, IntegerType::class));
    }

    public function test_type_is_any_on_union_type() : void
    {
        $type = type_union(type_integer(), type_boolean(), type_string());

        self::assertTrue(type_is_any($type, UnionType::class, OptionalType::class));
        self::assertTrue(type_is_any($type, IntegerType::class, StringType::class));
        self::assertTrue(type_is_any($type, StringType::class, BooleanType::class));
        self::assertTrue(type_is_any($type, BooleanType::class, IntegerType::class));
        self::assertFalse(type_is_any($type, FloatType::class, MapType::class));
    }

    public function test_type_is_on_optional_type() : void
    {
        $type = type_optional(type_string());

        self::assertTrue(type_is($type, StringType::class));
        self::assertTrue(type_is($type, OptionalType::class));
        self::assertFalse(type_is($type, IntegerType::class));
    }

    public function test_type_is_on_union_type() : void
    {
        $type = type_union(type_integer(), type_boolean(), type_string());

        self::assertTrue(type_is($type, UnionType::class));
        self::assertTrue(type_is($type, IntegerType::class));
        self::assertTrue(type_is($type, StringType::class));
        self::assertTrue(type_is($type, BooleanType::class));
        self::assertFalse(type_is($type, FloatType::class));
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     */
    #[DataProvider('type_not_comparable_data_provider')]
    public function test_type_not_comparable(Type $left, Type $right) : void
    {
        self::assertFalse((new Comparator())->comparable($left, $right));
    }
}
