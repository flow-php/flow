<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use function Flow\ETL\DSL\{type_array, type_boolean, type_float, type_int, type_integer, type_null, type_object, type_string};
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Types;
use Flow\ETL\Tests\FlowTestCase;

final class TypesTest extends FlowTestCase
{
    public function test_all() : void
    {
        $types = new Types(
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
        $types = new Types(
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

    public function test_first() : void
    {
        $types = new Types(
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
        $types = new Types(
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
    }

    public function test_only() : void
    {
        $types = new Types(
            type_string(),
            type_int(),
            type_float(),
            type_boolean(),
            type_array(),
            type_object(ObjectType::class),
            type_null(),
        );

        self::assertCount(7, $types);
        self::assertEquals(new Types(type_string()), $types->only(type_string()));
        self::assertEquals(new Types(type_integer()), $types->only(type_integer()));
        self::assertEquals(new Types(type_float()), $types->only(type_float()));
        self::assertEquals(new Types(type_boolean()), $types->only(type_boolean()));
        self::assertEquals(new Types(type_array()), $types->only(type_array()));
        self::assertEquals(new Types(type_object(ObjectType::class)), $types->only(type_object(ObjectType::class)));
        self::assertEquals(new Types(type_null()), $types->only(type_null()));
    }
}
