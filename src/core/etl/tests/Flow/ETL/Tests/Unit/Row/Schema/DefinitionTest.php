<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use function Flow\ETL\DSL\{bool_schema, date_schema, float_schema, integer_schema, json_schema, list_schema, map_schema, string_schema, structure_schema, time_schema, type_integer};
use function Flow\ETL\DSL\{datetime_schema,
    int_entry,
    int_schema,
    str_entry,
    struct_entry,
    struct_schema,
    type_float,
    type_int,
    type_list,
    type_map,
    type_string,
    type_structure};
use Flow\ETL\Exception\{RuntimeException};
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\{Metadata};
use Flow\ETL\Tests\FlowTestCase;

final class DefinitionTest extends FlowTestCase
{
    public function test_compatibility_with_differences_in_metadat() : void
    {
        $definition = integer_schema('id', metadata: Metadata::fromArray(['test' => 'test']));

        self::assertTrue(
            $definition->isCompatible(
                integer_schema('id', false, Metadata::fromArray(['description' => 'some_random_description']))
            )
        );
    }

    public function test_compatibility_with_differences_in_nullability_that_is_a_backward_compatibility_break() : void
    {
        $definition = integer_schema('id', metadata: Metadata::fromArray(['test' => 'test']));

        self::assertFalse(
            $definition->isCompatible(
                integer_schema('id', true, Metadata::fromArray(['description' => 'some_random_description']))
            )
        );
    }

    public function test_equals_nullability() : void
    {
        $def = integer_schema('id', nullable: true);

        self::assertFalse(
            $def->isSame(
                integer_schema('id', nullable: false)
            )
        );
        self::assertTrue(
            $def->isSame(
                integer_schema('id', nullable: true)
            )
        );
    }

    public function test_equals_types() : void
    {
        $def = list_schema('list', type_list(type_integer()));

        self::assertTrue(
            $def->isSame(
                list_schema('list', type_list(type_integer()))
            )
        );
    }

    public function test_matches_when_type_and_name_match() : void
    {
        $def = integer_schema('test');

        self::assertTrue($def->matches(int_entry('test', 1)));
    }

    public function test_merge_definitions() : void
    {
        self::assertEquals(
            integer_schema('id', true),
            integer_schema('id')->merge(integer_schema('id', true))
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
            integer_schema('id', true),
            integer_schema('id', false)->merge(string_schema('id', true, Metadata::fromArray([Metadata::FROM_NULL => true])))
        );
        self::assertEquals(
            float_schema('id', true),
            float_schema('id', false)->merge(string_schema('id', true, Metadata::fromArray([Metadata::FROM_NULL => true])))
        );
        self::assertEquals(
            bool_schema('id', true),
            bool_schema('id', false)->merge(string_schema('id', true, Metadata::fromArray([Metadata::FROM_NULL => true])))
        );
        self::assertEquals(
            datetime_schema('id', true),
            datetime_schema('id', false)->merge(string_schema('id', true, Metadata::fromArray([Metadata::FROM_NULL => true])))
        );
    }

    public function test_merging_anything_and_string() : void
    {
        self::assertEquals(
            string_schema('id', true),
            integer_schema('id', false)->merge(string_schema('id', true))
        );
        self::assertEquals(
            string_schema('id', true),
            float_schema('id', false)->merge(string_schema('id', true))
        );
        self::assertEquals(
            string_schema('id', true),
            bool_schema('id', false)->merge(string_schema('id', true))
        );
        self::assertEquals(
            string_schema('id', true),
            datetime_schema('id', false)->merge(string_schema('id', true))
        );
    }

    public function test_merging_date_with_datetime() : void
    {
        self::assertEquals(
            datetime_schema('datetime'),
            datetime_schema('datetime')->merge(date_schema('datetime'))
        );
    }

    public function test_merging_different_entries() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, int and string');

        integer_schema('int')->merge(string_schema('string'));
    }

    public function test_merging_float_and_int_definition() : void
    {
        self::assertEquals(
            float_schema('id', false, precision: 3),
            float_schema('id', precision: 3)->merge(int_schema('id'))
        );

        self::assertEquals(
            float_schema('id', true, precision: 4),
            float_schema('id', precision: 4)->merge(int_schema('id', true))
        );
    }

    public function test_merging_float_definitions_with_different_precisions() : void
    {
        self::assertEquals(
            float_schema('id', true),
            float_schema('id')->merge(float_schema('id', true, 3))
        );

        self::assertEquals(
            float_schema('id', true, 12),
            float_schema('id', false, 1)->merge(float_schema('id', true, 12))
        );
    }

    public function test_merging_list_of_ints_and_floats() : void
    {
        self::assertEquals(
            list_schema('list', type_list(type_float())),
            list_schema('list', type_list(type_int()))->merge(list_schema('list', type_list(type_float())))
        );
    }

    public function test_merging_numeric_types() : void
    {
        self::assertEquals(
            float_schema('id', true),
            integer_schema('id', false)->merge(float_schema('id', true))
        );
        self::assertEquals(
            float_schema('id', true),
            float_schema('id', false)->merge(integer_schema('id', true))
        );
    }

    public function test_merging_time_with_date() : void
    {
        self::assertEquals(
            datetime_schema('datetime'),
            date_schema('datetime')->merge(time_schema('datetime'))
        );
    }

    public function test_merging_time_with_datetime() : void
    {
        self::assertEquals(
            datetime_schema('datetime'),
            datetime_schema('datetime')->merge(time_schema('datetime'))
        );
    }

    public function test_merging_two_definitions_created_from_null() : void
    {
        self::assertTrue(
            string_schema('id', true, Metadata::fromArray([Metadata::FROM_NULL => true]))
                ->merge(string_schema('id', true, Metadata::fromArray([Metadata::FROM_NULL => true])))
                ->metadata()->has(Metadata::FROM_NULL)
        );
    }

    public function test_merging_two_different_lists() : void
    {
        self::assertEquals(
            json_schema('list'),
            list_schema('list', type_list(type_string()))->merge(list_schema('list', type_list(type_int())))
        );
    }

    public function test_merging_two_different_maps() : void
    {
        self::assertEquals(
            json_schema('map'),
            map_schema('map', type_map(type_string(), type_string()))->merge(map_schema('map', type_map(type_string(), type_int())))
        );
    }

    public function test_merging_two_different_structures() : void
    {
        self::assertEquals(
            json_schema('structure'),
            structure_schema('structure', type_structure([
                'street' => type_string(),
                'city' => type_string(),
            ]))->merge(
                structure_schema('structure', type_structure([
                    'street' => type_string(),
                    'city' => type_int(),
                ]))
            )
        );
    }

    public function test_merging_two_same_lists() : void
    {
        self::assertEquals(
            list_schema('list', type_list(type_int())),
            list_schema('list', type_list(type_int()))->merge(list_schema('list', type_list(type_int())))
        );
    }

    public function test_merging_two_same_maps() : void
    {
        self::assertEquals(
            map_schema('map', type_map(type_string(), type_string())),
            map_schema('map', type_map(type_string(), type_string()))->merge(map_schema('map', type_map(type_string(), type_string())))
        );
    }

    public function test_normalize_and_from_array() : void
    {
        $definition = struct_schema(
            'structure',
            type_structure(
                [
                    'street' => type_string(),
                    'city' => type_string(),
                    'location' => type_structure(
                        [
                            'lat' => type_float(),
                            'lng' => type_float(),
                        ]
                    ),
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
        $def = integer_schema('test', $nullable = false);

        self::assertFalse($def->matches(str_entry('test', null)));
    }

    public function test_not_matches_when_type_does_not_match() : void
    {
        $def = integer_schema('test');

        self::assertFalse($def->matches(str_entry('test', 'test')));
    }

    public function test_not_matches_when_type_name_not_match() : void
    {
        $def = integer_schema('test');

        self::assertFalse($def->matches(int_entry('not-test', 1)));
    }

    public function test_set_metadata() : void
    {
        $definition = integer_schema('id', metadata: Metadata::fromArray(['test' => 'test']));

        self::assertEquals(
            integer_schema('id', false, Metadata::fromArray(['description' => 'some_random_description'])),
            $definition->setMetadata(Metadata::fromArray(['description' => 'some_random_description']))
        );

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
            type_structure([
                'street' => type_string(),
                'city' => type_string(),
                'location' => type_structure([
                    'lat' => type_float(),
                    'lng' => type_float(),
                ]),
            ]),
        );

        self::assertEquals(
            type_structure([
                'street' => type_string(),
                'city' => type_string(),
                'location' => type_structure([
                    'lat' => type_float(),
                    'lng' => type_float(),
                ]),
            ]),
            $address->definition()->type()
        );
    }
}
