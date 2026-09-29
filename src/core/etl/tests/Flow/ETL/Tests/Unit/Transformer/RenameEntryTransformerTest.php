<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\RenameEntryTransformer;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_metadata;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_json;

final class RenameEntryTransformerTest extends FlowTestCase
{
    public function test_renaming_keeps_the_column_position_and_its_values(): void
    {
        $renamed = (new RenameEntryTransformer('a', 'x'))->transform(
            array_to_rows([['a' => 1, 'b' => 2]], schema(integer_schema('a'), integer_schema('b'))),
            flow_context(config()),
        );

        static::assertSame(['x', 'b'], array_keys($renamed->values(0)));
        static::assertSame([['x' => 1, 'b' => 2]], $renamed->toArray());
    }

    public function test_bind_renames_the_column_in_place(): void
    {
        static::assertEquals(
            schema(integer_schema('new_int'), string_schema('status')),
            (new RenameEntryTransformer('old_int', 'new_int'))->bind(schema(
                integer_schema('old_int'),
                string_schema('status'),
            ))->output,
        );
    }

    public function test_bind_renaming_a_column_to_its_own_name_returns_the_input_schema(): void
    {
        $input = schema(integer_schema('id'));

        static::assertEquals($input, (new RenameEntryTransformer('id', 'id'))->bind($input)->output);
    }

    public function test_renaming_entries(): void
    {
        $context = flow_context(config());

        $rows = (new RenameEntryTransformer('old_int', 'new_int'))->transform(
            array_to_rows(
                [[
                    'old_int' => 1000,
                    'id' => 1,
                    'status' => 'PENDING',
                    'enabled' => true,
                    'datetime' => type_datetime()->cast('2020-01-01 00:00:00 UTC'),
                    'json' => type_json()->cast(['foo', 'bar']),
                    'null' => null,
                ]],
                schema(
                    integer_schema('old_int'),
                    integer_schema('id'),
                    string_schema('status'),
                    bool_schema('enabled'),
                    datetime_schema('datetime'),
                    json_schema('json'),
                    string_schema('null', nullable: true),
                ),
            ),
            $context,
        );

        static::assertEquals(
            array_to_rows(
                [[
                    'new_int' => 1000,
                    'id' => 1,
                    'status' => 'PENDING',
                    'enabled' => true,
                    'datetime' => type_datetime()->cast('2020-01-01 00:00:00 UTC'),
                    'json' => type_json()->cast(['foo', 'bar']),
                    'nothing' => null,
                ]],
                schema(
                    integer_schema('new_int'),
                    integer_schema('id'),
                    string_schema('status'),
                    bool_schema('enabled'),
                    datetime_schema('datetime'),
                    json_schema('json'),
                    string_schema('nothing', nullable: true),
                ),
            ),
            (new RenameEntryTransformer('null', 'nothing'))->transform($rows, $context),
        );
    }

    public function test_renaming_entry_preserves_metadata(): void
    {
        $metadata = schema_metadata(['description' => 'test metadata', 'priority' => 1]);

        $outputRows = (new RenameEntryTransformer('old_name', 'new_name'))->transform(array_to_rows([[
            'old_name' => 'test value',
        ]], schema(string_schema('old_name', metadata: $metadata))), flow_context(config()));

        static::assertSame('test value', $outputRows->column('new_name')->value(0));
        static::assertTrue($outputRows->schema()->get('new_name')->metadata()->isEqual($metadata));
    }

    public function test_renaming_to_same_name_returns_same_rows_instance(): void
    {
        $inputRows = array_to_rows([['name' => 'value']], schema(string_schema('name')));

        static::assertSame($inputRows, (new RenameEntryTransformer('name', 'name'))->transform(
            $inputRows,
            flow_context(config()),
        ));
    }
}
