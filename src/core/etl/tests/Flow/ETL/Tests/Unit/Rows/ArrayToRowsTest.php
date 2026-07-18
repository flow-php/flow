<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rows;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class ArrayToRowsTest extends FlowTestCase
{
    public function test_building_array_to_rows_with_entry_that_is_list_of_strings(): void
    {
        $rows = array_to_rows([
            ['data' => ['a', 'b', 'c', 'd']],
            ['data' => ['e', 'f', 'g', 'd']],
        ], flow_context(config())->hydrator());

        static::assertEquals(
            rows(
                row(list_entry('data', ['a', 'b', 'c', 'd'], type_list(type_string()))),
                row(list_entry('data', ['e', 'f', 'g', 'd'], type_list(type_string()))),
            ),
            $rows,
        );
    }

    public function test_building_array_to_rows_with_entry_that_is_list_of_strings_with_one_row(): void
    {
        $rows = array_to_rows([
            ['data' => ['e', 'f', 'g', 'd']],
        ], flow_context(config())->hydrator());

        static::assertEquals(rows(row(list_entry('data', ['e', 'f', 'g', 'd'], type_list(type_string())))), $rows);
    }

    public function test_building_row_from_array_with_schema_and_additional_fields_not_covered_by_schema(): void
    {
        $rows = array_to_rows(
            ['id' => 1234, 'deleted' => false, 'phase' => null],
            flow_context(config())->hydrator(),
            schema: schema(int_schema('id'), bool_schema('deleted')),
        );

        static::assertEquals(rows(row(int_entry('id', 1234), bool_entry('deleted', false))), $rows);
    }

    public function test_building_row_from_array_with_schema_but_entries_not_available_in_rows(): void
    {
        $rows = array_to_rows(
            ['id' => 1234, 'deleted' => false],
            flow_context(config())->hydrator(),
            schema: schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', true)),
        );

        static::assertEquals(
            rows(row(int_entry('id', 1234), bool_entry('deleted', false), str_entry('phase', null))),
            $rows,
        );
    }

    public function test_building_rows_from_array(): void
    {
        $rows = array_to_rows([
            ['id' => 1234, 'deleted' => false, 'phase' => null],
            ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
        ], flow_context(config())->hydrator());

        static::assertEquals(
            rows(
                row(int_entry('id', 1234), bool_entry('deleted', false), null_entry('phase')),
                row(int_entry('id', 4321), bool_entry('deleted', true), str_entry('phase', 'launch')),
            ),
            $rows,
        );
    }

    public function test_building_rows_from_array_with_schema_and_additional_fields_not_covered_by_schema(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            flow_context(config())->hydrator(),
            schema: schema(int_schema('id'), bool_schema('deleted')),
        );

        static::assertEquals(
            rows(
                row(int_entry('id', 1234), bool_entry('deleted', false)),
                row(int_entry('id', 4321), bool_entry('deleted', true)),
            ),
            $rows,
        );
    }

    public function test_building_rows_from_array_with_schema_but_entries_not_available_in_rows(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false],
                ['id' => 4321, 'deleted' => true],
            ],
            flow_context(config())->hydrator(),
            schema: schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', true)),
        );

        static::assertEquals(
            rows(
                row(int_entry('id', 1234), bool_entry('deleted', false), str_entry('phase', null)),
                row(int_entry('id', 4321), bool_entry('deleted', true), str_entry('phase', null)),
            ),
            $rows,
        );
    }
}
