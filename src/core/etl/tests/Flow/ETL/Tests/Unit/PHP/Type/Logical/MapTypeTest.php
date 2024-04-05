<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\Map\{MapKey, MapValue};
use Flow\ETL\PHP\Type\Logical\{ListType, MapType};
use PHPUnit\Framework\TestCase;

final class MapTypeTest extends TestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            (new MapType(MapKey::string(), MapValue::float()))->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        self::assertFalse(
            (new MapType(MapKey::string(), MapValue::float()))->isEqual(new ListType(ListElement::integer()))
        );
        self::assertFalse(
            (new MapType(MapKey::string(), MapValue::float()))->isEqual(new MapType(MapKey::string(), MapValue::integer()))
        );
    }

    public function test_key() : void
    {
        self::assertEquals(
            $key = MapKey::string(),
            (new MapType($key, MapValue::float()))->key()
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'map<string, string>',
            (new MapType(MapKey::string(), MapValue::string()))->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            (new MapType(MapKey::string(), MapValue::string()))->isValid(['one' => 'two'])
        );
        self::assertTrue(
            (new MapType(MapKey::string(), MapValue::string(), true))->isValid(null)
        );
        self::assertTrue(
            (new MapType(MapKey::integer(), MapValue::list(new ListType(ListElement::integer()))))->isValid([[1, 2], [3, 4]])
        );
        self::assertTrue(
            (
                new MapType(
                    MapKey::integer(),
                    MapValue::map(new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::integer()))))
                )
            )->isValid([0 => ['one' => [1, 2]], 1 => ['two' => [3, 4]]])
        );
        self::assertFalse(
            (new MapType(MapKey::integer(), MapValue::string()))->isValid(['one' => 'two'])
        );
        self::assertFalse(
            (new MapType(MapKey::integer(), MapValue::string()))->isValid([1, 2])
        );
        self::assertFalse(
            (new MapType(MapKey::string(), MapValue::string()))->isValid(123)
        );
    }

    public function test_value() : void
    {
        self::assertEquals(
            $value = MapValue::string(),
            (new MapType(MapKey::string(), $value))->value()
        );
    }
}
