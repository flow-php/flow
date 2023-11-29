<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use PHPUnit\Framework\TestCase;

final class StructureTypeTest extends TestCase
{
    public function test_elements() : void
    {
        $this->assertEquals(
            [$map = new StructureElement('map', new MapType(MapKey::string(), MapValue::float()))],
            (new StructureType($map))->elements()
        );
    }

    public function test_equals() : void
    {
        $this->assertTrue(
            (new StructureType(new StructureElement('map', new MapType(MapKey::string(), MapValue::float()))))
                ->isEqual(new StructureType(new StructureElement('map', new MapType(MapKey::string(), MapValue::float()))))
        );
        $this->assertFalse(
            (new StructureType(new StructureElement('string', type_string()), new StructureElement('bool', type_boolean())))
                ->isEqual(new ListType(ListElement::integer()))
        );
        $this->assertFalse(
            (new StructureType(new StructureElement('string', type_string()), new StructureElement('bool', type_boolean())))
                ->isEqual(new StructureType(new StructureElement('bool', type_boolean()), new StructureElement('integer', type_string())))
        );
    }

    public function test_structure_element_name_cannot_be_empty() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Structure element name cannot be empty');

        new StructureElement('', type_string());
    }

    public function test_structure_elements_must_have_unique_names() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('All structure element names must be unique');

        (new StructureType(
            new StructureElement('test', type_string()),
            new StructureElement('test', type_string())
        ));
    }

    public function test_to_string() : void
    {
        $struct = new StructureType(
            new StructureElement('string', type_string()),
            new StructureElement('float', type_float()),
            new StructureElement('map', new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::object(\DateTimeInterface::class)))))
        );

        $this->assertSame(
            'structure{string: string, float: float, map: map<string, list<object<DateTimeInterface>>>}',
            $struct->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            (new StructureType(new StructureElement('string', type_string())))->isValid(['one' => 'two'])
        );
        $this->assertTrue(
            (
                new StructureType(
                    new StructureElement(
                        'map',
                        new MapType(
                            MapKey::integer(),
                            MapValue::map(new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::integer()))))
                        )
                    ),
                    new StructureElement('string', type_string()),
                    new StructureElement('float', type_float())
                )
            )->isValid(['a' => [0 => ['one' => [1, 2]], 1 => ['two' => [3, 4]]], 'b' => 'c', 'd' => 1.5])
        );
        $this->assertFalse(
            (new StructureType(new StructureElement('int', type_int())))->isValid([1, 2])
        );
    }
}
