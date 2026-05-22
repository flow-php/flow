<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\struct_entry;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayToRowTest extends FlowTestCase
{
    public function test_building_array_to_row_with_entry_that_is_list_of_strings(): void
    {
        $row = array_to_row(['data' => ['a', 'b', 'c', 'd']], flow_context(config())->entryFactory());

        static::assertEquals(row(list_entry('data', ['a', 'b', 'c', 'd'], type_list(type_string()))), $row);
    }

    public function test_building_single_row_from_array_with_rows_fails(): void
    {
        $row = array_to_row([
            ['id' => 1234, 'deleted' => false, 'phase' => null],
            ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
        ], flow_context(config())->entryFactory());

        static::assertEquals(
            row(
                // @mago-ignore analysis:less-specific-argument
                struct_entry('e00', ['id' => 1234, 'deleted' => false, 'phase' => null], type_structure([
                    'id' => type_integer(),
                    'deleted' => type_boolean(),
                    'phase' => type_null(),
                ])),
                // @mago-ignore analysis:less-specific-argument
                struct_entry('e01', ['id' => 4321, 'deleted' => true, 'phase' => 'launch'], type_structure([
                    'id' => type_integer(),
                    'deleted' => type_boolean(),
                    'phase' => type_string(),
                ])),
            ),
            $row,
        );
    }

    public function test_building_single_row_from_array_with_schema_and_additional_fields_not_covered_by_schema(): void
    {
        $row = array_to_row(
            ['id' => 1234, 'deleted' => false, 'phase' => null],
            flow_context(config())->entryFactory(),
            schema: schema(int_schema('id'), bool_schema('deleted')),
        );

        static::assertEquals(row(int_entry('id', 1234), bool_entry('deleted', false)), $row);
    }

    public function test_building_single_row_from_array_with_schema_but_entries_not_available_in_rows(): void
    {
        $row = array_to_row(
            ['id' => 1234, 'deleted' => false],
            flow_context(config())->entryFactory(),
            schema: schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', true)),
        );

        static::assertEquals(row(int_entry('id', 1234), bool_entry('deleted', false), str_entry('phase', null)), $row);
    }

    public function test_building_single_row_from_flat_array(): void
    {
        $row = array_to_row([
            'id' => 1234,
            'deleted' => false,
            'phase' => null,
        ], flow_context(config())->entryFactory());

        static::assertEquals(row(int_entry('id', 1234), bool_entry('deleted', false), null_entry('phase')), $row);
    }
}
