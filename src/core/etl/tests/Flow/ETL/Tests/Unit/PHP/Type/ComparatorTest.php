<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use function Flow\ETL\DSL\{type_boolean, type_equals, type_float, type_int, type_integer, type_is, type_is_any, type_list, type_map, type_null, type_optional, type_string, type_structure, type_union};
use Flow\ETL\PHP\Type\{Comparator, Type};
use Flow\ETL\PHP\Type\Logical\{MapType, OptionalType};
use Flow\ETL\PHP\Type\Native\{BooleanType, FloatType, IntegerType, ResourceType, StringType, UnionType};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class ComparatorTest extends FlowTestCase
{
    public static function type_comparable_data_provider() : \Generator
    {
        yield [type_int(), type_int()];
        yield [type_int(), type_float()];
        yield [type_float(), type_int()];
        yield [type_float(), type_float()];

        yield [type_int(), type_optional(type_int())];
        yield [type_float(), type_optional(type_float())];

        yield [type_union(type_int(), type_null()), type_int()];
        yield [type_union(type_int(), type_null()), type_float()];
        yield [type_int(), type_string()];
    }

    public static function type_comparison_data_provider() : \Generator
    {
        yield [type_int(), type_float(), false];
        yield [type_int(), type_string(), false];
        yield [type_null(), type_optional(type_boolean()), false];
        yield [type_int(), type_int(), true];
        yield [type_float(), type_float(), true];
        yield [type_string(), type_string(), true];
        yield [type_optional(type_integer()), type_optional(type_integer()), true];
        yield [type_optional(type_integer()), type_optional(type_float()), false];
        yield [type_map(type_string(), type_int()), type_map(type_string(), type_int()), true];
        yield [type_map(type_string(), type_int()), type_map(type_int(), type_int()), false];
        yield [type_map(type_string(), type_int()), type_map(type_string(), type_float()), false];
        yield [type_list(type_string()), type_list(type_string()), true];
        yield [type_list(type_string()), type_list(type_int()), false];
        yield [type_list(type_int()), type_list(type_optional(type_int())), false];
        yield [type_structure(['id' => type_int(), 'name' => type_string()]), type_structure(['id' => type_int(), 'name' => type_string()]), true];
        yield [type_structure(['id' => type_int(), 'name' => type_string()]), type_structure(['id' => type_int(), 'name' => type_optional(type_string())]), false];
        yield [type_structure(['name' => type_string()]), type_structure(['id' => type_int(), 'name' => type_string()]), false];
        yield [type_structure(['id' => type_int(), 'name' => type_string()]), type_structure(['name' => type_string()]), false];
        yield [type_structure(['id' => type_int(), 'name' => type_string()]), type_structure(['id' => type_int(), 'name' => type_string(), 'active' => type_boolean()]), false];
    }

    public static function type_not_comparable_data_provider() : \Generator
    {
        yield [type_int(), type_union(type_float(), type_int())];
        yield [type_int(), type_boolean()];
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
