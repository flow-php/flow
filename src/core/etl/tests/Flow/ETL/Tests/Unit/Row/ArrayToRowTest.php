<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;

final class ArrayToRowTest extends FlowTestCase
{
    public function test_building_array_to_row_with_entry_that_is_list_of_strings(): void
    {
        $row = array_to_row(
            ['data' => ['a', 'b', 'c', 'd']],
            schema(list_schema('data', type_list(type_string()))),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(row(['data' => ['a', 'b', 'c', 'd']]), $row);
    }

    public function test_building_single_row_from_array_with_rows_fails(): void
    {
        $row = array_to_row(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            schema(
                map_schema('e00', type_map(type_string(), type_mixed()), nullable: true),
                map_schema('e01', type_map(type_string(), type_mixed()), nullable: true),
            ),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(
            row([
                'e00' => ['id' => 1234, 'deleted' => false, 'phase' => null],
                'e01' => ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ]),
            $row,
        );
    }

    public function test_building_single_row_from_array_with_schema_and_additional_fields_not_covered_by_schema(): void
    {
        $row = array_to_row(
            ['id' => 1234, 'deleted' => false, 'phase' => null],
            schema(int_schema('id'), bool_schema('deleted')),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(row(['id' => 1234, 'deleted' => false]), $row);
    }

    public function test_building_single_row_from_array_with_schema_but_entries_not_available_in_rows(): void
    {
        $row = array_to_row(
            ['id' => 1234, 'deleted' => false],
            schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', true)),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(row(['id' => 1234, 'deleted' => false, 'phase' => null]), $row);
    }

    public function test_building_single_row_from_flat_array(): void
    {
        $row = array_to_row(
            [
                'id' => 1234,
                'deleted' => false,
                'phase' => null,
            ],
            schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', true)),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(row(['id' => 1234, 'deleted' => false, 'phase' => null]), $row);
    }
}
