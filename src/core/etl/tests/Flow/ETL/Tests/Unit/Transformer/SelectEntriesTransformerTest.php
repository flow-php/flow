<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_json;

final class SelectEntriesTransformerTest extends FlowTestCase
{
    public function test_bind_projects_and_reorders_to_the_selected_columns(): void
    {
        static::assertEquals(
            schema(str_schema('name'), int_schema('id')),
            (new SelectEntriesTransformer('name', 'id'))->bind(schema(
                int_schema('id'),
                str_schema('name'),
                json_schema('array'),
            ))->output,
        );
    }

    public function test_bind_refuses_a_column_missing_from_the_input(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "not_existing" not found');

        (new SelectEntriesTransformer('not_existing'))->bind(schema(int_schema('id')));
    }

    public function test_selecting_entries(): void
    {
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]],
            schema(int_schema('id'), str_schema('name'), json_schema('array')),
        );

        $transformer = new SelectEntriesTransformer('name');
        static::assertSame(
            [
                ['name' => 'Row Name'],
            ],
            $transformer->transform($rows, flow_context(config()))->toArray(),
        );
    }

    public function test_selecting_not_existing_entries(): void
    {
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]],
            schema(int_schema('id'), string_schema('name'), json_schema('array')),
        );

        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "not_existing" not found');

        (new SelectEntriesTransformer('not_existing'))->transform($rows, flow_context(config()));
    }

    public function test_selecting_an_entry_missing_from_some_rows_widens_to_a_nullable_real_type(): void
    {
        $transformer = new SelectEntriesTransformer('id');

        static::assertEquals(
            schema(int_schema('id', nullable: true)),
            $transformer
                ->transform(
                    array_to_rows(
                        [['id' => 1], ['name' => 'no id here']],
                        schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                    ),
                    flow_context(config()),
                )
                ->schema(),
        );
    }

    public function test_using_select_entries_in_order_to_change_entries_order(): void
    {
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]],
            schema(int_schema('id'), str_schema('name'), json_schema('array')),
        );

        $result = (new SelectEntriesTransformer('name', 'id', 'array'))->transform($rows, flow_context(config()));

        static::assertSame(['name', 'id', 'array'], $result->schema()->references()->names());
        static::assertSame(
            [
                ['name' => 'Row Name', 'id' => 1, 'array' => ['test']],
            ],
            $result->toArray(),
        );
    }

    public function test_select_carries_a_not_null_definition_through_unchanged(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            (new SelectEntriesTransformer('id'))
                ->transform(
                    array_to_rows(
                        [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']],
                        schema(int_schema('id'), str_schema('name')),
                    ),
                    flow_context(config()),
                )
                ->schema(),
        );
    }
}
