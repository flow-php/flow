<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type\Logical\InstanceOfType;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_resource;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\types;

final class TypesTest extends TestCase
{
    public function test_all(): void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_instance_of(InstanceOfType::class),
            type_null(),
        );

        static::assertEquals($types->all(), [
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_instance_of(InstanceOfType::class),
            type_null(),
        ]);
    }

    public function test_count(): void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_instance_of(InstanceOfType::class),
            type_null(),
        );

        static::assertCount(7, $types);
    }

    public function test_deduplication(): void
    {
        static::assertEquals(types(type_integer()), types(type_integer(), type_integer())->deduplicate());
        static::assertEquals(
            types(type_union(type_string(), type_integer())),
            types(type_union(type_integer(), type_string()), type_union(type_string(), type_integer()))->deduplicate(),
        );
    }

    public function test_first(): void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_instance_of(InstanceOfType::class),
            type_null(),
        );

        static::assertEquals($types->first(), type_string());
    }

    public function test_has(): void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_instance_of(InstanceOfType::class),
            type_null(),
        );

        static::assertTrue($types->has(type_string()));
        static::assertTrue($types->has(type_integer()));
        static::assertTrue($types->has(type_float()));
        static::assertTrue($types->has(type_boolean()));
        static::assertTrue($types->has(type_array()));
        static::assertTrue($types->has(type_instance_of(InstanceOfType::class)));
        static::assertTrue($types->has(type_null()));
        static::assertFalse($types->has(type_resource()));
    }

    public function test_has_all(): void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_instance_of(InstanceOfType::class),
            type_null(),
        );

        static::assertTrue($types->hasAll(type_string(), type_integer()));
        static::assertTrue($types->hasAll(type_float(), type_boolean()));
        static::assertTrue($types->hasAll(type_array(), type_instance_of(InstanceOfType::class)));
        static::assertTrue($types->hasAll(type_null()));
        static::assertFalse($types->hasAll(type_string(), type_resource()));
    }

    public function test_has_any(): void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_instance_of(InstanceOfType::class),
            type_null(),
        );

        static::assertTrue($types->hasAny(type_string(), type_integer()));
        static::assertTrue($types->hasAny(type_float(), type_boolean()));
        static::assertTrue($types->hasAny(type_resource(), type_array(), type_instance_of(InstanceOfType::class)));
        static::assertTrue($types->hasAny(type_null()));
        static::assertFalse($types->hasAny(type_resource()));
    }

    public function test_only(): void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_instance_of(InstanceOfType::class),
            type_null(),
        );

        static::assertCount(7, $types);
        static::assertEquals(types(type_string()), $types->only(type_string()));
        static::assertEquals(types(type_integer()), $types->only(type_integer()));
        static::assertEquals(types(type_float()), $types->only(type_float()));
        static::assertEquals(types(type_boolean()), $types->only(type_boolean()));
        static::assertEquals(types(type_array()), $types->only(type_array()));
        static::assertEquals(
            types(type_instance_of(InstanceOfType::class)),
            $types->only(type_instance_of(InstanceOfType::class)),
        );
        static::assertEquals(types(), $types->only(type_instance_of(stdClass::class)));
        static::assertEquals(types(type_null()), $types->only(type_null()));
    }

    public function test_reduce_optionals(): void
    {
        $types = types(type_optional(type_string()), type_boolean());

        static::assertEquals(types(type_string(), type_boolean()), $types->reduceOptionals());
    }

    public function test_reduce_optionals_with_optional_union_types(): void
    {
        $types = types(type_union(type_string(), type_null()), type_union(type_string(), type_null(), type_integer()));

        static::assertEquals(
            types(type_string(), type_union(type_string(), type_null(), type_integer())),
            $types->reduceOptionals(),
        );
    }

    public function test_without(): void
    {
        $types = types(
            type_string(),
            type_integer(),
            type_float(),
            type_boolean(),
            type_array(),
            type_instance_of(InstanceOfType::class),
            type_null(),
        );

        static::assertEquals(
            types(
                type_integer(),
                type_float(),
                type_boolean(),
                type_array(),
                type_instance_of(InstanceOfType::class),
                type_null(),
            ),
            $types->without(type_string()),
        );
    }
}
