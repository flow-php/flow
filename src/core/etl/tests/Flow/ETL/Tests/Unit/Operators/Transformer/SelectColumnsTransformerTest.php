<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Operators\Transformer\SelectColumnsTransformer;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_json;

final class SelectColumnsTransformerTest extends FlowTestCase
{
    public function test_bind_projects_and_reorders_to_the_selected_columns(): void
    {
        static::assertEquals(
            schema(str_schema('name'), int_schema('id')),
            (new SelectColumnsTransformer('name', 'id'))->bind(schema(
                int_schema('id'),
                str_schema('name'),
                json_schema('array'),
            ))->output,
        );
    }

    public function test_bind_names_an_aliased_column_by_its_alias_with_the_read_column_definition(): void
    {
        static::assertEquals(
            schema(int_schema('key'), str_schema('name')),
            (new SelectColumnsTransformer(ref('id')->as('key'), 'name'))->bind(schema(
                int_schema('id'),
                str_schema('name'),
            ))->output,
        );
    }

    public function test_bind_refuses_two_references_that_output_one_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('select() would output column "name" twice');

        (new SelectColumnsTransformer(ref('id')->as('name'), 'name'))->bind(schema(
            int_schema('id'),
            str_schema('name'),
        ));
    }

    public function test_bind_refuses_a_column_missing_from_the_input(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "not_existing" not found');

        (new SelectColumnsTransformer('not_existing'))->bind(schema(int_schema('id')));
    }

    public function test_selecting_entries(): void
    {
        static::assertSame(
            [
                ['name' => 'Row Name'],
            ],
            (new SelectColumnsTransformer('name'))
                ->transform(
                    array_to_rows(
                        [['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]],
                        schema(int_schema('id'), str_schema('name'), json_schema('array')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_selecting_not_existing_entries(): void
    {
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]],
            schema(int_schema('id'), string_schema('name'), json_schema('array')),
        );

        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "not_existing" not found');

        (new SelectColumnsTransformer('not_existing'))->transform($rows, flow_context(config())->backend());
    }

    public function test_selecting_an_entry_missing_from_some_rows_widens_to_a_nullable_real_type(): void
    {
        static::assertEquals(
            schema(int_schema('id', nullable: true)),
            (new SelectColumnsTransformer('id'))
                ->transform(
                    array_to_rows(
                        [['id' => 1], ['name' => 'no id here']],
                        schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                    ),
                    flow_context(config())->backend(),
                )
                ->schema(),
        );
    }

    public function test_using_select_entries_in_order_to_change_entries_order(): void
    {
        $result = (new SelectColumnsTransformer('name', 'id', 'array'))->transform(
            array_to_rows(
                [['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]],
                schema(int_schema('id'), str_schema('name'), json_schema('array')),
            ),
            flow_context(config())->backend(),
        );

        static::assertSame(['name', 'id', 'array'], $result->schema()->names());
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
            (new SelectColumnsTransformer('id'))
                ->transform(
                    array_to_rows(
                        [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']],
                        schema(int_schema('id'), str_schema('name')),
                    ),
                    flow_context(config())->backend(),
                )
                ->schema(),
        );
    }

    public function test_describe_round_trips(): void
    {
        $transformer = new SelectColumnsTransformer('name', 'id');
        $rebuilt = DescribeRoundTrip::rebuilt($transformer);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );
        $schema = schema(int_schema('id'), str_schema('name'));

        static::assertSame(
            StepRoundTrip::transformed($transformer, $schema, $rows, flow_context()),
            StepRoundTrip::transformed($rebuilt, $schema, $rows, flow_context()),
        );
    }
}
