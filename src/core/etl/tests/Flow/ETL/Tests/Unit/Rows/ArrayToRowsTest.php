<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rows;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
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
            flow_context(config())->backend(),
        );

        static::assertEquals(
            array_to_rows([
                ['data' => ['a', 'b', 'c', 'd']],
                ['data' => ['e', 'f', 'g', 'd']],
            ], schema(list_schema('data', type_list(type_string())))),
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
            flow_context(config())->backend(),
        );

        static::assertEquals(
            array_to_rows([['data' => ['e', 'f', 'g', 'd']]], schema(list_schema('data', type_list(type_string())))),
            $rows,
        );
    }

    public function test_refuses_an_undeclared_key_of_a_single_flat_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "phase" (row 0) is not declared by the schema',
        );

        array_to_rows(
            ['id' => 1234, 'deleted' => false, 'phase' => null],
            schema(int_schema('id'), bool_schema('deleted')),
            flow_context(config())->backend(),
        );
    }

    public function test_building_row_from_array_with_schema_but_entries_not_available_in_rows(): void
    {
        $rows = array_to_rows(
            ['id' => 1234, 'deleted' => false],
            schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', true)),
            flow_context(config())->backend(),
        );

        static::assertEquals(
            array_to_rows(
                [['id' => 1234, 'deleted' => false, 'phase' => null]],
                schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', nullable: true)),
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
            flow_context(config())->backend(),
        );

        static::assertEquals(
            array_to_rows(
                [
                    ['id' => 1234, 'deleted' => false, 'phase' => null],
                    ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
                ],
                schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', nullable: true)),
            ),
            $rows,
        );
    }

    public function test_refuses_an_undeclared_key(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "nmae" (row 1) is not declared by the schema',
        );

        array_to_rows(
            [
                ['id' => 1234, 'name' => 'launch'],
                ['id' => 4321, 'nmae' => 'landing'],
            ],
            schema(int_schema('id'), str_schema('name', nullable: true)),
            flow_context(config())->backend(),
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
            flow_context(config())->backend(),
        );

        static::assertEquals(
            array_to_rows(
                [
                    ['id' => 1234, 'deleted' => false, 'phase' => null],
                    ['id' => 4321, 'deleted' => true, 'phase' => null],
                ],
                schema(int_schema('id'), bool_schema('deleted'), str_schema('phase', nullable: true)),
            ),
            $rows,
        );
    }

    public function test_a_numeric_key_is_the_declared_column_of_that_name(): void
    {
        static::assertSame(
            [['0' => 'x', 'e07' => 'y']],
            array_to_rows(
                [[0 => 'x', 7 => 'y']],
                schema(str_schema('0'), str_schema('e07')),
                flow_context(config())->backend(),
            )->toArray(),
        );
    }

    public function test_refuses_an_undeclared_numeric_key_under_its_positional_name(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "e07" (row 0) is not declared by the schema',
        );

        array_to_rows([[0 => 'x', 7 => 'y']], schema(str_schema('0')), flow_context(config())->backend());
    }
}
