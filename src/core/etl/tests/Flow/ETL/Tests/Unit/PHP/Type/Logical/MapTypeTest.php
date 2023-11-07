<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use PHPUnit\Framework\TestCase;

final class MapTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            (new MapType(MapKey::string(), MapValue::float()))->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            (new MapType(MapKey::string(), MapValue::float()))->isEqual(new ListType(ListElement::integer()))
        );
        $this->assertFalse(
            (new MapType(MapKey::string(), MapValue::float()))->isEqual(new MapType(MapKey::string(), MapValue::integer()))
        );
    }

    public function test_key() : void
    {
        $this->assertEquals(
            $key = MapKey::string(),
            (new MapType($key, MapValue::float()))->key()
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'map<string, string>',
            (new MapType(MapKey::string(), MapValue::string()))->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            (new MapType(MapKey::string(), MapValue::string()))->isValid(['one' => 'two'])
        );
        $this->assertTrue(
            (new MapType(MapKey::integer(), MapValue::list(new ListType(ListElement::integer()))))->isValid([[1, 2]])
        );
        $this->assertTrue(
            (new MapType(MapKey::integer(), MapValue::map(new MapType(MapKey::integer(), MapValue::integer()))))->isValid([[1, 2]])
        );
        $this->assertFalse(
            (new MapType(MapKey::integer(), MapValue::string()))->isValid(['one' => 'two'])
        );
        $this->assertFalse(
            (new MapType(MapKey::integer(), MapValue::string()))->isValid([1, 2])
        );
        $this->assertFalse(
            (new MapType(MapKey::string(), MapValue::string()))->isValid(123)
        );
    }

    public function test_value() : void
    {
        $this->assertEquals(
            $value = MapValue::string(),
            (new MapType(MapKey::string(), $value))->value()
        );
    }
}
