<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\partition;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class ArrayToRowTest extends FlowTestCase
{
    public function test_building_array_to_row_with_entry_that_is_list_of_strings(): void
    {
        $row = array_to_row(
            ['data' => ['a', 'b', 'c', 'd']],
            schema(list_schema('data', type_list(type_string()))),
            flow_context(config())->backend(),
        );

        static::assertSame(['data' => ['a', 'b', 'c', 'd']], $row->toArray());
    }

    public function test_building_single_row_from_array_with_rows_fails(): void
    {
        $row = array_to_row(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            schema(json_schema('e00', nullable: true), json_schema('e01', nullable: true)),
            flow_context(config())->backend(),
        );

        static::assertSame(
            [
                'e00' => ['id' => 1234, 'deleted' => false, 'phase' => null],
                'e01' => ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            $row->toArray(),
        );
    }

    public function test_refuses_an_undeclared_key(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "phase" (row 0) is not declared by the schema',
        );

        array_to_row(
            ['id' => 1234, 'deleted' => false, 'phase' => null],
            schema(int_schema('id'), bool_schema('deleted')),
            flow_context(config())->backend(),
        );
    }

    public function test_refuses_an_undeclared_partition(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "year" (row 0) is not declared by the schema',
        );

        array_to_row(['id' => 1234], schema(int_schema('id')), flow_context(config())->backend(), [partition(
            'year',
            '2024',
        )]);
    }

    public function test_building_single_row_from_array_with_schema_but_entries_not_available_in_rows(): void
    {
        $row = array_to_row(
            ['id' => 1234, 'deleted' => false],
            schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', true)),
            flow_context(config())->backend(),
        );

        static::assertSame(['id' => 1234, 'deleted' => false, 'phase' => null], $row->toArray());
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
            flow_context(config())->backend(),
        );

        static::assertSame(['id' => 1234, 'deleted' => false, 'phase' => null], $row->toArray());
    }
}
