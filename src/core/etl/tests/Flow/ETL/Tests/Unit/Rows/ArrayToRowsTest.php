<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rows;

use function Flow\ETL\DSL\{array_to_rows,
    bool_entry,
    bool_schema,
    int_entry,
    int_schema,
    list_entry,
    null_entry,
    row,
    rows,
    str_entry,
    str_schema,
    type_list,
    type_string};
use Flow\ETL\Row\Schema;
use PHPUnit\Framework\TestCase;

final class ArrayToRowsTest extends TestCase
{
    public function test_building_array_to_rows_with_entry_that_is_list_of_strings() : void
    {
        $rows = array_to_rows(
            [
                ['data' => ['a', 'b', 'c', 'd']],
                ['data' => ['e', 'f', 'g', 'd']],
            ]
        );

        self::assertEquals(
            rows(
                row(
                    list_entry('data', ['a', 'b', 'c', 'd'], type_list(type_string())),
                ),
                row(
                    list_entry('data', ['e', 'f', 'g', 'd'], type_list(type_string())),
                ),
            ),
            $rows
        );
    }

    public function test_building_array_to_rows_with_entry_that_is_list_of_strings_with_one_row() : void
    {
        $rows = array_to_rows(
            [
                ['data' => ['e', 'f', 'g', 'd']],
            ]
        );

        self::assertEquals(
            rows(
                row(
                    list_entry('data', ['e', 'f', 'g', 'd'], type_list(type_string())),
                ),
            ),
            $rows
        );
    }

    public function test_building_row_from_array_with_schema_and_additional_fields_not_covered_by_schema() : void
    {
        $rows = array_to_rows(
            ['id' => 1234, 'deleted' => false, 'phase' => null],
            schema: new Schema(
                int_schema('id'),
                bool_schema('deleted'),
            )
        );

        self::assertEquals(
            rows(
                row(
                    int_entry('id', 1234),
                    bool_entry('deleted', false),
                ),
            ),
            $rows
        );
    }

    public function test_building_row_from_array_with_schema_but_entries_not_available_in_rows() : void
    {
        $rows = array_to_rows(
            ['id' => 1234, 'deleted' => false],
            schema: new Schema(
                int_schema('id'),
                bool_schema('deleted'),
                str_schema('phase', true),
            )
        );

        self::assertEquals(
            rows(
                row(
                    int_entry('id', 1234),
                    bool_entry('deleted', false),
                    str_entry('phase', null)
                ),
            ),
            $rows
        );
    }

    public function test_building_rows_from_array() : void
    {
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ]
        );

        self::assertEquals(
            rows(
                row(
                    int_entry('id', 1234),
                    bool_entry('deleted', false),
                    null_entry('phase'),
                ),
                row(
                    int_entry('id', 4321),
                    bool_entry('deleted', true),
                    str_entry('phase', 'launch'),
                )
            ),
            $rows
        );
    }

    public function test_building_rows_from_array_with_schema_and_additional_fields_not_covered_by_schema() : void
    {
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            schema: new Schema(
                int_schema('id'),
                bool_schema('deleted'),
            )
        );

        self::assertEquals(
            rows(
                row(
                    int_entry('id', 1234),
                    bool_entry('deleted', false),
                ),
                row(
                    int_entry('id', 4321),
                    bool_entry('deleted', true),
                )
            ),
            $rows
        );
    }

    public function test_building_rows_from_array_with_schema_but_entries_not_available_in_rows() : void
    {
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false],
                ['id' => 4321, 'deleted' => true],
            ],
            schema: new Schema(
                int_schema('id'),
                bool_schema('deleted'),
                str_schema('phase', true),
            )
        );

        self::assertEquals(
            rows(
                row(
                    int_entry('id', 1234),
                    bool_entry('deleted', false),
                    str_entry('phase', null)
                ),
                row(
                    int_entry('id', 4321),
                    bool_entry('deleted', true),
                    str_entry('phase', null)
                )
            ),
            $rows
        );
    }
}
