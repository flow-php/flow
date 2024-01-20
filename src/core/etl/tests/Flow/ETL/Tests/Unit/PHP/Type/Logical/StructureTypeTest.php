<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_type;
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
use PHPUnit\Framework\TestCase;

final class StructureTypeTest extends TestCase
{
    public function test_elements() : void
    {
        $this->assertEquals(
            $map = [struct_element('map', new MapType(MapKey::string(), MapValue::float()))],
            (struct_type($map))->elements()
        );
    }

    public function test_equals() : void
    {
        $this->assertTrue(
            (struct_type([struct_element('map', new MapType(MapKey::string(), MapValue::float()))]))
                ->isEqual(struct_type([struct_element('map', new MapType(MapKey::string(), MapValue::float()))]))
        );
        $this->assertFalse(
            (struct_type([struct_element('string', type_string()), struct_element('bool', type_boolean())]))
                ->isEqual(new ListType(ListElement::integer()))
        );
        $this->assertFalse(
            (struct_type([struct_element('string', type_string()), struct_element('bool', type_boolean())]))
                ->isEqual(struct_type([struct_element('bool', type_boolean()), struct_element('integer', type_string())]))
        );
        $this->assertTrue(
            struct_type([
                struct_element('string', type_string()),
                struct_element('bool', type_boolean()),
            ])
            ->isEqual(
                struct_type([
                    struct_element('string', type_string()),
                    struct_element('bool', type_boolean()),
            ])
            )
        );
        $this->assertFalse(
            struct_type([
                struct_element('string', type_string()),
                struct_element('bool', type_boolean()),
            ])
                ->isEqual(
                    struct_type([
                        struct_element('string', type_string()),
                        struct_element('bool', type_boolean(true)),
                    ])
                )
        );
    }

    public function test_structure_element_name_cannot_be_empty() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Structure element name cannot be empty');

        struct_element('', type_string());
    }

    public function test_structure_elements_must_have_unique_names() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('All structure element names must be unique');

        (struct_type([
            struct_element('test', type_string()),
            struct_element('test', type_string()),
        ]));
    }

    public function test_to_string() : void
    {
        $struct = struct_type([
            struct_element('string', type_string()),
            struct_element('float', type_float()),
            struct_element('map', new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::datetime())))),
        ]);

        $this->assertSame(
            'structure{string: string, float: float, map: map<string, list<datetime>>}',
            $struct->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            (struct_type([struct_element('string', type_string())]))->isValid(['one' => 'two'])
        );
        $this->assertTrue(
            (
                struct_type([
                    struct_element(
                        'map',
                        new MapType(
                            MapKey::integer(),
                            MapValue::map(new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::integer()))))
                        )
                    ),
                    struct_element('string', type_string()),
                    struct_element('float', type_float()),
                ])
            )->isValid(['a' => [0 => ['one' => [1, 2]], 1 => ['two' => [3, 4]]], 'b' => 'c', 'd' => 1.5])
        );
        $this->assertFalse(
            (struct_type([struct_element('int', type_int())]))->isValid([1, 2])
        );
    }
}
