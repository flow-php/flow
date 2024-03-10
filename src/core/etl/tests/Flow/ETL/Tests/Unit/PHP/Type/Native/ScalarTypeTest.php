<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_boolean, type_float, type_int, type_string};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\Map\{MapKey, MapValue};
use Flow\ETL\PHP\Type\Logical\MapType;
use PHPUnit\Framework\TestCase;

final class ScalarTypeTest extends TestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_int()->isEqual(type_int())
        );
        self::assertFalse(
            type_int()->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        self::assertFalse(
            type_int()->isEqual(type_float())
        );
    }

    public function test_merge_different_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot merge different types, string and integer');

        type_string()->merge(type_int());
    }

    public function test_merge_same_types() : void
    {
        self::assertTrue(
            type_int()->merge(type_int())->isEqual(type_int())
        );
        self::assertTrue(
            type_string()->merge(type_string())->isEqual(type_string())
        );
        self::assertTrue(
            type_boolean()->merge(type_boolean())->isEqual(type_boolean())
        );
        self::assertTrue(
            type_float()->merge(type_float())->isEqual(type_float())
        );
    }

    public function test_merge_with_null() : void
    {
        self::assertTrue(
            type_int()->merge(type_int(true))->isEqual(type_int(true))
        );
        self::assertTrue(
            type_string()->merge(type_string(true))->isEqual(type_string(true))
        );
        self::assertTrue(
            type_boolean()->merge(type_boolean(true))->isEqual(type_boolean(true))
        );
        self::assertTrue(
            type_float()->merge(type_float(true))->isEqual(type_float(true))
        );
    }

    public function test_nullable() : void
    {
        self::assertFalse(
            type_string(false)->nullable()
        );
        self::assertTrue(
            type_boolean(true)->nullable()
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'boolean',
            type_boolean()->toString()
        );
        self::assertSame(
            '?string',
            type_string(true)->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_boolean()->isValid(true)
        );
        self::assertTrue(
            type_string()->isValid('one')
        );
        self::assertTrue(
            type_int()->isValid(1)
        );
        self::assertTrue(
            type_int(true)->isValid(null)
        );
        self::assertFalse(
            type_int()->isValid('one')
        );
        self::assertFalse(
            type_string()->isValid([1, 2])
        );
        self::assertFalse(
            type_boolean()->isValid(123)
        );
    }
}
