<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_json;

final class SelectEntriesTransformerTest extends FlowTestCase
{
    public function test_selecting_entries(): void
    {
        $rows = rows(
            schema(int_schema('id'), str_schema('name'), json_schema('array')),
            row(['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]),
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
        $rows = rows(
            schema(int_schema('id'), string_schema('name'), json_schema('array')),
            row(['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]),
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
                    rows(
                        schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                        row(['id' => 1]),
                        row(['name' => 'no id here']),
                    ),
                    flow_context(config()),
                )
                ->schema(),
        );
    }

    public function test_using_select_entries_in_order_to_change_entries_order(): void
    {
        $rows = rows(
            schema(int_schema('id'), str_schema('name'), json_schema('array')),
            row(['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]),
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
                    rows(
                        schema(int_schema('id'), str_schema('name')),
                        row(['id' => 1, 'name' => 'Alice']),
                        row(['id' => 2, 'name' => 'Bob']),
                    ),
                    flow_context(config()),
                )
                ->schema(),
        );
    }
}
