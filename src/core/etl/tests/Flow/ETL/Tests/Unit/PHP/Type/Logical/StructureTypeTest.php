<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{struct_element, struct_type, type_boolean, type_float, type_int, type_string};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\Map\{MapKey, MapValue};
use Flow\ETL\PHP\Type\Logical\{ListType, MapType};
use PHPUnit\Framework\TestCase;

final class StructureTypeTest extends TestCase
{
    public function test_elements() : void
    {
        self::assertEquals(
            $map = [struct_element('map', new MapType(MapKey::string(), MapValue::float()))],
            (struct_type($map))->elements()
        );
    }

    public function test_equals() : void
    {
        self::assertTrue(
            (struct_type([struct_element('map', new MapType(MapKey::string(), MapValue::float()))]))
                ->isEqual(struct_type([struct_element('map', new MapType(MapKey::string(), MapValue::float()))]))
        );
        self::assertFalse(
            (struct_type([struct_element('string', type_string()), struct_element('bool', type_boolean())]))
                ->isEqual(new ListType(ListElement::integer()))
        );
        self::assertFalse(
            (struct_type([struct_element('string', type_string()), struct_element('bool', type_boolean())]))
                ->isEqual(struct_type([struct_element('bool', type_boolean()), struct_element('integer', type_string())]))
        );
        self::assertTrue(
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
        self::assertFalse(
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

    public function test_merging_different_left_structure() : void
    {
        self::assertEquals(
            struct_type([
                struct_element('string', type_string(true)),
                struct_element('float', type_float()),
                struct_element('map', new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::datetime())), true)),
            ]),
            struct_type([
                struct_element('string', type_string()),
                struct_element('float', type_float()),
            ])->merge(struct_type([
                struct_element('float', type_float()),
                struct_element('map', new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::datetime())))),
            ]))
        );
    }

    public function test_merging_different_right_structure() : void
    {
        self::assertEquals(
            struct_type([
                struct_element('string', type_string(true)),
                struct_element('float', type_float(true)),
                struct_element('map', new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::datetime())), true)),
            ]),
            struct_type([
                struct_element('string', type_string()),
                struct_element('float', type_float()),
            ])->merge(struct_type([
                struct_element('map', new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::datetime())))),
            ]))
        );
    }

    public function test_merging_nested_structures() : void
    {
        self::assertEquals(
            struct_type([
                struct_element('string', type_string(true)),
                struct_element('float', type_float(true)),
                struct_element('structure', struct_type(
                    [
                        struct_element('id', type_string()),
                        struct_element('name', type_float()),
                    ],
                    nullable: true
                )),
            ]),
            struct_type([
                struct_element('string', type_string()),
                struct_element('float', type_float()),
            ])->merge(
                struct_type([
                    struct_element('structure', struct_type(
                        [
                            struct_element('id', type_string()),
                            struct_element('name', type_float()),
                        ],
                    )),
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

        self::assertSame(
            'structure{string: string, float: float, map: map<string, list<datetime>>}',
            $struct->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            (struct_type([struct_element('string', type_string())]))->isValid(['one' => 'two'])
        );
        self::assertTrue(
            (struct_type([struct_element('string', type_string())], true))->isValid(null)
        );
        self::assertTrue(
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
        self::assertFalse(
            (struct_type([struct_element('int', type_int())]))->isValid([1, 2])
        );
    }
}
