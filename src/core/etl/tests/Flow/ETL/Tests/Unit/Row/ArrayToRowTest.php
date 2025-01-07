<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\{array_to_row,
    bool_entry,
    bool_schema,
    int_entry,
    int_schema,
    list_entry,
    null_entry,
    row,
    str_entry,
    str_schema,
    struct_entry,
    type_boolean,
    type_int,
    type_list,
    type_null,
    type_string,
    type_structure};
use Flow\ETL\Tests\FlowTestCase;

final class ArrayToRowTest extends FlowTestCase
{
    public function test_building_array_to_row_with_entry_that_is_list_of_strings() : void
    {
        $row = array_to_row(['data' => ['a', 'b', 'c', 'd']]);

        self::assertEquals(
            row(list_entry('data', ['a', 'b', 'c', 'd'], type_list(type_string()))),
            $row
        );
    }

    public function test_building_single_row_from_array_with_rows_fails() : void
    {
        $row = array_to_row(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ]
        );

        self::assertEquals(
            row(
                struct_entry(
                    'e00',
                    ['id' => 1234, 'deleted' => false, 'phase' => null],
                    type_structure([
                        'id' => type_int(),
                        'deleted' => type_boolean(),
                        'phase' => type_null(),
                    ])
                ),
                struct_entry(
                    'e01',
                    ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
                    type_structure([
                        'id' => type_int(),
                        'deleted' => type_boolean(),
                        'phase' => type_string(),
                    ])
                )
            ),
            $row
        );
    }

    public function test_building_single_row_from_array_with_schema_and_additional_fields_not_covered_by_schema() : void
    {
        $row = array_to_row(
            ['id' => 1234, 'deleted' => false, 'phase' => null],
            schema: schema(int_schema('id'), bool_schema('deleted'))
        );

        self::assertEquals(
            row(
                int_entry('id', 1234),
                bool_entry('deleted', false),
            ),
            $row
        );
    }

    public function test_building_single_row_from_array_with_schema_but_entries_not_available_in_rows() : void
    {
        $row = array_to_row(
            ['id' => 1234, 'deleted' => false],
            schema: schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', true))
        );

        self::assertEquals(
            row(
                int_entry('id', 1234),
                bool_entry('deleted', false),
                str_entry('phase', null)
            ),
            $row
        );
    }

    public function test_building_single_row_from_flat_array() : void
    {
        $row = array_to_row(
            ['id' => 1234, 'deleted' => false, 'phase' => null],
        );

        self::assertEquals(
            row(
                int_entry('id', 1234),
                bool_entry('deleted', false),
                null_entry('phase'),
            ),
            $row
        );
    }
}
