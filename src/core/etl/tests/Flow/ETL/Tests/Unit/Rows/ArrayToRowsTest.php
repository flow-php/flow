<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rows;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class ArrayToRowsTest extends FlowTestCase
{
    public function test_building_array_to_rows_with_entry_that_is_list_of_strings(): void
    {
        $rows = array_to_rows(
            [
                ['data' => ['a', 'b', 'c', 'd']],
                ['data' => ['e', 'f', 'g', 'd']],
            ],
            schema(list_schema('data', type_list(type_string()))),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(
            rows(
                schema(list_schema('data', type_list(type_string()))),
                row(['data' => ['a', 'b', 'c', 'd']]),
                row(['data' => ['e', 'f', 'g', 'd']]),
            ),
            $rows,
        );
    }

    public function test_building_array_to_rows_with_entry_that_is_list_of_strings_with_one_row(): void
    {
        $rows = array_to_rows(
            [
                ['data' => ['e', 'f', 'g', 'd']],
            ],
            schema(list_schema('data', type_list(type_string()))),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(
            rows(schema(list_schema('data', type_list(type_string()))), row(['data' => ['e', 'f', 'g', 'd']])),
            $rows,
        );
    }

    public function test_building_row_from_array_with_schema_and_additional_fields_not_covered_by_schema(): void
    {
        $rows = array_to_rows(
            ['id' => 1234, 'deleted' => false, 'phase' => null],
            schema(int_schema('id'), bool_schema('deleted')),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(
            rows(schema(int_schema('id'), bool_schema('deleted')), row(['id' => 1234, 'deleted' => false])),
            $rows,
        );
    }

    public function test_building_row_from_array_with_schema_but_entries_not_available_in_rows(): void
    {
        $rows = array_to_rows(
            ['id' => 1234, 'deleted' => false],
            schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', true)),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(
            rows(
                schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', nullable: true)),
                row(['id' => 1234, 'deleted' => false, 'phase' => null]),
            ),
            $rows,
        );
    }

    public function test_building_rows_from_array(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', nullable: true)),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(
            rows(
                schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', nullable: true)),
                row(['id' => 1234, 'deleted' => false, 'phase' => null]),
                row(['id' => 4321, 'deleted' => true, 'phase' => 'launch']),
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
            schema(int_schema('id'), bool_schema('deleted')),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(
            rows(
                schema(int_schema('id'), bool_schema('deleted')),
                row(['id' => 1234, 'deleted' => false]),
                row(['id' => 4321, 'deleted' => true]),
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
            schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', true)),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(
            rows(
                schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', nullable: true)),
                row(['id' => 1234, 'deleted' => false, 'phase' => null]),
                row(['id' => 4321, 'deleted' => true, 'phase' => null]),
            ),
            $rows,
        );
    }
}
