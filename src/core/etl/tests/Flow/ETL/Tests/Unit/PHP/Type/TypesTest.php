<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use function Flow\ETL\DSL\{type_array,
    type_boolean,
    type_float,
    type_int,
    type_integer,
    type_null,
    type_object,
    type_resource,
    type_string,
    type_union,
    types};
use Flow\ETL\PHP\Type\Native\{ObjectType};
use Flow\ETL\Tests\FlowTestCase;

final class TypesTest extends FlowTestCase
{
    public function test_all() : void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_object(ObjectType::class),
            type_null(),
        );

        self::assertEquals($types->all(), [
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_object(ObjectType::class),
            type_null(),
        ]);
    }

    public function test_count() : void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_object(ObjectType::class),
            type_null(),
        );

        self::assertCount(7, $types);
    }

    public function test_deduplication() : void
    {
        self::assertEquals(
            types(type_int()),
            types(type_int(), type_integer())->deduplicate()
        );
        self::assertEquals(
            types(type_union(type_string(), type_int())),
            types(type_union(type_int(), type_string()), type_union(type_string(), type_int()))->deduplicate()
        );
    }

    public function test_first() : void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_object(ObjectType::class),
            type_null(),
        );

        self::assertEquals($types->first(), type_string());
    }

    public function test_has() : void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_object(ObjectType::class),
            type_null(),
        );

        self::assertTrue($types->has(type_string()));
        self::assertTrue($types->has(type_integer()));
        self::assertTrue($types->has(type_float()));
        self::assertTrue($types->has(type_boolean()));
        self::assertTrue($types->has(type_array()));
        self::assertTrue($types->has(type_object(ObjectType::class)));
        self::assertTrue($types->has(type_null()));
        self::assertFalse($types->has(type_resource()));
    }

    public function test_has_all() : void
    {
        $types = types(
            type_string(),
            type_int(),
            type_float(),
            type_boolean(),
            type_array(),
            type_object(ObjectType::class),
            type_null(),
        );

        self::assertTrue($types->hasAll(type_string(), type_int()));
        self::assertTrue($types->hasAll(type_float(), type_boolean()));
        self::assertTrue($types->hasAll(type_array(), type_object(ObjectType::class)));
        self::assertTrue($types->hasAll(type_null()));
        self::assertFalse($types->hasAll(type_string(), type_resource()));
    }

    public function test_has_any() : void
    {
        $types = types(
            type_string(),
            type_int(),
            type_float(),
            type_boolean(),
            type_array(),
            type_object(ObjectType::class),
            type_null(),
        );

        self::assertTrue($types->hasAny(type_string(), type_int()));
        self::assertTrue($types->hasAny(type_float(), type_boolean()));
        self::assertTrue($types->hasAny(type_resource(), type_array(), type_object(ObjectType::class)));
        self::assertTrue($types->hasAny(type_null()));
        self::assertFalse($types->hasAny(type_resource()));
    }

    public function test_only() : void
    {
        $types = types(
            type_string(),
            type_int(),
            type_float(),
            type_boolean(),
            type_array(),
            type_object(ObjectType::class),
            type_null(),
        );

        self::assertCount(7, $types);
        self::assertEquals(types(type_string()), $types->only(type_string()));
        self::assertEquals(types(type_integer()), $types->only(type_integer()));
        self::assertEquals(types(type_float()), $types->only(type_float()));
        self::assertEquals(types(type_boolean()), $types->only(type_boolean()));
        self::assertEquals(types(type_array()), $types->only(type_array()));
        self::assertEquals(types(type_object(ObjectType::class)), $types->only(type_object(ObjectType::class)));
        self::assertEquals(types(), $types->only(type_object(\stdClass::class)));
        self::assertEquals(types(type_null()), $types->only(type_null()));
    }

    public function test_reduce_optionals() : void
    {
        $types = types(
            type_string(true),
            type_boolean(true),
        );

        self::assertEquals(
            types(
                type_string(),
                type_boolean(),
            ),
            $types->reduceOptionals()
        );
    }

    public function test_reduce_optionals_with_optional_union_types() : void
    {
        $types = types(
            type_union(type_string(), type_null()),
            type_union(type_string(), type_null(), type_integer()),
        );

        self::assertEquals(
            types(
                type_string(),
                type_union(type_string(), type_null(), type_integer()),
            ),
            $types->reduceOptionals()
        );
    }

    public function test_without() : void
    {
        $types = types(
            type_string(),
            type_int(),
            type_float(),
            type_boolean(),
            type_array(),
            type_object(ObjectType::class),
            type_null(),
        );

        self::assertEquals(
            types(
                type_int(),
                type_float(),
                type_boolean(),
                type_array(),
                type_object(ObjectType::class),
                type_null(),
            ),
            $types->without(type_string())
        );
    }
}
