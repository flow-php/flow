<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_array, type_float};
use Flow\ETL\PHP\Type\Logical\Map\{MapKey, MapValue};
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use PHPUnit\Framework\TestCase;

final class ArrayTypeTest extends TestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            (type_array())->isEqual(new ArrayType)
        );
        self::assertTrue(
            ArrayType::empty()->isEqual(ArrayType::empty())
        );
        self::assertFalse(
            (type_array())->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        self::assertFalse(
            (type_array())->isEqual(type_float())
        );
        self::assertFalse(
            ArrayType::empty()->isEqual(type_array())
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'array<mixed>',
            type_array()->toString()
        );
        self::assertSame(
            'array<empty, empty>',
            ArrayType::empty()->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_array()->isValid([])
        );
        self::assertTrue(
            type_array()->isValid(['one'])
        );
        self::assertTrue(
            type_array()->isValid([1])
        );
        self::assertFalse(
            type_array()->isValid(null)
        );
        self::assertFalse(
            type_array()->isValid('one')
        );
        self::assertFalse(
            type_array()->isValid(true)
        );
        self::assertFalse(
            type_array()->isValid(123)
        );
    }
}
