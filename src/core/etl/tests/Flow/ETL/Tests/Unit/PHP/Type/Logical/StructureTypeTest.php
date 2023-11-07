<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use PHPUnit\Framework\TestCase;

final class StructureTypeTest extends TestCase
{
    public function test_elements() : void
    {
        $this->assertEquals(
            [$map = new MapType(MapKey::string(), MapValue::float())],
            (new StructureType($map))->elements()
        );
    }

    public function test_equals() : void
    {
        $this->assertTrue(
            (new StructureType(new MapType(MapKey::string(), MapValue::float())))->isEqual(new StructureType(new MapType(MapKey::string(), MapValue::float())))
        );
        $this->assertFalse(
            (new StructureType(ScalarType::string, ScalarType::boolean))->isEqual(new ListType(ListElement::integer()))
        );
        $this->assertFalse(
            (new StructureType(ScalarType::string, ScalarType::boolean))->isEqual(new StructureType(ScalarType::boolean, ScalarType::string))
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'structure<string, float, map<string, list<object<DateTimeInterface>>>>',
            (new StructureType(ScalarType::string, ScalarType::float, new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::object(\DateTimeInterface::class))))))->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            (new StructureType(ScalarType::string))->isValid(['one' => 'two'])
        );
        $this->assertTrue(
            (
                new StructureType(
                    new MapType(
                        MapKey::integer(),
                        MapValue::map(new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::integer()))))
                    ),
                    ScalarType::string,
                    ScalarType::float
                )
            )->isValid(['a' => [0 => ['one' => [1, 2]], 1 => ['two' => [3, 4]]], 'b' => 'c', 'd' => 1.5])
        );
        $this->assertFalse(
            (new StructureType(ScalarType::integer))->isValid([1, 2])
        );
    }
}
