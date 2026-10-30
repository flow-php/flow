<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Operators\Transformer\DropColumnsTransformer;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_json;

final class DropColumnsTransformerTest extends FlowTestCase
{
    public function test_bind_ignores_a_column_missing_from_the_input(): void
    {
        $input = schema(int_schema('id'));

        static::assertEquals($input, (new DropColumnsTransformer('not_existing'))->bind($input)->output);
    }

    public function test_bind_removes_the_dropped_columns(): void
    {
        static::assertEquals(
            schema(string_schema('name')),
            (new DropColumnsTransformer('id', 'array'))->bind(schema(
                int_schema('id'),
                string_schema('name'),
                json_schema('array'),
            ))->output,
        );
    }

    public function test_dropping_entries(): void
    {
        static::assertSame(
            [
                ['name' => 'Row Name'],
            ],
            (new DropColumnsTransformer('id', 'array'))
                ->transform(
                    array_to_rows(
                        [['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]],
                        schema(int_schema('id'), string_schema('name'), json_schema('array')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_removing_not_existing_entries(): void
    {
        static::assertSame(
            [
                ['id' => 1, 'name' => 'Row Name', 'array' => ['test']],
            ],
            (new DropColumnsTransformer('not_existing'))
                ->transform(
                    array_to_rows(
                        [['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]],
                        schema(int_schema('id'), string_schema('name'), json_schema('array')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_describe_round_trips(): void
    {
        $transformer = new DropColumnsTransformer('name');
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
