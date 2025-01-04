<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use function Flow\ETL\DSL\{datetime_schema,
    int_entry,
    str_entry,
    struct_element,
    struct_entry,
    struct_schema,
    struct_type,
    type_datetime,
    type_float,
    type_int,
    type_list,
    type_map,
    type_string,
    type_structure};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\{ListType, StructureType};
use Flow\ETL\Row\Schema\{Definition, Metadata};
use Flow\ETL\Tests\FlowTestCase;

final class DefinitionTest extends FlowTestCase
{
    public function test_creating_definition_without_class() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry class "DateTimeInterface" must implement "Flow\ETL\Row\Entry"');

        new Definition('name', \DateTimeInterface::class, type_datetime());
    }

    public function test_equals_nullability() : void
    {
        $def = Definition::integer('id', nullable: true);

        self::assertFalse(
            $def->isEqual(
                Definition::integer('id', nullable: false)
            )
        );
        self::assertTrue(
            $def->isEqual(
                Definition::integer('id', nullable: true)
            )
        );
    }

    public function test_equals_types() : void
    {
        $def = Definition::list('list', new ListType(ListElement::integer()));

        self::assertTrue(
            $def->isEqual(
                Definition::list('list', new ListType(ListElement::integer()))
            )
        );
    }

    public function test_matches_when_type_and_name_match() : void
    {
        $def = Definition::integer('test');

        self::assertTrue($def->matches(int_entry('test', 1)));
    }

    public function test_merge_definitions() : void
    {
        self::assertEquals(
            Definition::integer('id', true),
            Definition::integer('id')->merge(Definition::integer('id', true))
        );
    }

    public function test_merge_nullable_with_non_nullable_dateime_definitions() : void
    {
        self::assertEquals(
            datetime_schema('col', true),
            datetime_schema('col')->merge(datetime_schema('col', true))
        );

        self::assertEquals(
            datetime_schema('col'),
            datetime_schema('col')->merge(datetime_schema('col'))
        );
    }

    public function test_merging_anything_and_assumed_string() : void
    {
        self::assertEquals(
            Definition::integer('id', true),
            Definition::integer('id', false)->merge(Definition::string('id', true, Metadata::fromArray([Metadata::FROM_NULL => true])))
        );
        self::assertEquals(
            Definition::float('id', true),
            Definition::float('id', false)->merge(Definition::string('id', true, Metadata::fromArray([Metadata::FROM_NULL => true])))
        );
        self::assertEquals(
            Definition::boolean('id', true),
            Definition::boolean('id', false)->merge(Definition::string('id', true, Metadata::fromArray([Metadata::FROM_NULL => true])))
        );
        self::assertEquals(
            Definition::dateTime('id', true),
            Definition::dateTime('id', false)->merge(Definition::string('id', true, Metadata::fromArray([Metadata::FROM_NULL => true])))
        );
    }

    public function test_merging_anything_and_string() : void
    {
        self::assertEquals(
            Definition::string('id', true),
            Definition::integer('id', false)->merge(Definition::string('id', true))
        );
        self::assertEquals(
            Definition::string('id', true),
            Definition::float('id', false)->merge(Definition::string('id', true))
        );
        self::assertEquals(
            Definition::string('id', true),
            Definition::boolean('id', false)->merge(Definition::string('id', true))
        );
        self::assertEquals(
            Definition::string('id', true),
            Definition::dateTime('id', false)->merge(Definition::string('id', true))
        );
    }

    public function test_merging_date_with_datetime() : void
    {
        self::assertEquals(
            Definition::datetime('datetime'),
            Definition::datetime('datetime')->merge(Definition::date('datetime'))
        );
    }

    public function test_merging_different_entries() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, int and string');

        Definition::integer('int')->merge(Definition::string('string'));
    }

    public function test_merging_list_of_ints_and_floats() : void
    {
        self::assertEquals(
            Definition::list('list', type_list(type_float())),
            Definition::list('list', type_list(type_int()))->merge(Definition::list('list', type_list(type_float())))
        );
    }

    public function test_merging_numeric_types() : void
    {
        self::assertEquals(
            Definition::float('id', true),
            Definition::integer('id', false)->merge(Definition::float('id', true))
        );
        self::assertEquals(
            Definition::float('id', true),
            Definition::float('id', false)->merge(Definition::integer('id', true))
        );
    }

    public function test_merging_time_with_date() : void
    {
        self::assertEquals(
            Definition::datetime('datetime'),
            Definition::date('datetime')->merge(Definition::time('datetime'))
        );
    }

    public function test_merging_time_with_datetime() : void
    {
        self::assertEquals(
            Definition::datetime('datetime'),
            Definition::datetime('datetime')->merge(Definition::time('datetime'))
        );
    }

    public function test_merging_two_different_lists() : void
    {
        self::assertEquals(
            Definition::json('list'),
            Definition::list('list', type_list(type_string()))->merge(Definition::list('list', type_list(type_int())))
        );
    }

    public function test_merging_two_different_maps() : void
    {
        self::assertEquals(
            Definition::json('map'),
            Definition::map('map', type_map(type_string(), type_string()))->merge(Definition::map('map', type_map(type_string(), type_int())))
        );
    }

    public function test_merging_two_different_structures() : void
    {
        self::assertEquals(
            Definition::json('structure'),
            Definition::structure(
                'structure',
                struct_type([
                    struct_element('street', type_string()),
                    struct_element('city', type_string()),
                ])
            )->merge(
                Definition::structure(
                    'structure',
                    struct_type([
                        struct_element('street', type_string()),
                        struct_element('city', type_int()),
                    ])
                )
            )
        );
    }

    public function test_merging_two_same_lists() : void
    {
        self::assertEquals(
            Definition::list('list', type_list(type_int())),
            Definition::list('list', type_list(type_int()))->merge(Definition::list('list', type_list(type_int())))
        );
    }

    public function test_merging_two_same_maps() : void
    {
        self::assertEquals(
            Definition::map('map', type_map(type_string(), type_string())),
            Definition::map('map', type_map(type_string(), type_string()))->merge(Definition::map('map', type_map(type_string(), type_string())))
        );
    }

    public function test_normalize_and_from_array() : void
    {
        $definition = struct_schema(
            'structure',
            type_structure(
                [
                    struct_element('street', type_string()),
                    struct_element('city', type_string()),
                    struct_element('location', type_structure(
                        [
                            struct_element('lat', type_float()),
                            struct_element('lng', type_float()),
                        ]
                    )),
                ]
            ),
            Metadata::with('description', 'some_random_description')->add('priority', 1)
        );

        self::assertEquals(
            $definition,
            Definition::fromArray($definition->normalize())
        );
    }

    public function test_not_matches_when_not_nullable_name_matches_but_null_given() : void
    {
        $def = Definition::integer('test', $nullable = false);

        self::assertFalse($def->matches(str_entry('test', null)));
    }

    public function test_not_matches_when_type_does_not_match() : void
    {
        $def = Definition::integer('test');

        self::assertFalse($def->matches(str_entry('test', 'test')));
    }

    public function test_not_matches_when_type_name_not_match() : void
    {
        $def = Definition::integer('test');

        self::assertFalse($def->matches(int_entry('not-test', 1)));
    }

    public function test_structure_definition_metadata() : void
    {
        $address = struct_entry(
            'address',
            [
                'street' => 'street',
                'city' => 'city',
                'location' => ['lat' => 1.0, 'lng' => 1.0],
            ],
            struct_type([
                struct_element('street', type_string()),
                struct_element('city', type_string()),
                struct_element(
                    'location',
                    struct_type([
                        struct_element('lat', type_float()),
                        struct_element('lng', type_float()),
                    ])
                ),
            ]),
        );

        self::assertEquals(
            new StructureType([
                struct_element('street', type_string()),
                struct_element('city', type_string()),
                struct_element(
                    'location',
                    new StructureType([
                        struct_element('lat', type_float()),
                        struct_element('lng', type_float()),
                    ])
                ),
            ]),
            $address->definition()->type()
        );
    }
}
