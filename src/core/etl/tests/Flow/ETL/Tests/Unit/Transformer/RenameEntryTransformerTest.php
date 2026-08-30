<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\RenameEntryTransformer;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_metadata;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_json;

final class RenameEntryTransformerTest extends FlowTestCase
{
    public function test_renaming_entries(): void
    {
        $context = flow_context(config());

        $rows = (new RenameEntryTransformer('old_int', 'new_int'))->transform(
            rows(
                schema(
                    integer_schema('old_int'),
                    integer_schema('id'),
                    string_schema('status'),
                    bool_schema('enabled'),
                    datetime_schema('datetime'),
                    json_schema('json'),
                    string_schema('null', nullable: true),
                ),
                row([
                    'old_int' => 1000,
                    'id' => 1,
                    'status' => 'PENDING',
                    'enabled' => true,
                    'datetime' => type_datetime()->cast('2020-01-01 00:00:00 UTC'),
                    'json' => type_json()->cast(['foo', 'bar']),
                    'null' => null,
                ]),
            ),
            $context,
        );

        static::assertEquals(
            rows(
                schema(
                    integer_schema('new_int'),
                    integer_schema('id'),
                    string_schema('status'),
                    bool_schema('enabled'),
                    datetime_schema('datetime'),
                    json_schema('json'),
                    string_schema('nothing', nullable: true),
                ),
                row([
                    'new_int' => 1000,
                    'id' => 1,
                    'status' => 'PENDING',
                    'enabled' => true,
                    'datetime' => type_datetime()->cast('2020-01-01 00:00:00 UTC'),
                    'json' => type_json()->cast(['foo', 'bar']),
                    'nothing' => null,
                ]),
            ),
            (new RenameEntryTransformer('null', 'nothing'))->transform($rows, $context),
        );
    }

    public function test_renaming_entry_preserves_metadata(): void
    {
        $metadata = schema_metadata(['description' => 'test metadata', 'priority' => 1]);

        $outputRows = (new RenameEntryTransformer('old_name', 'new_name'))->transform(
            rows(schema(string_schema('old_name', metadata: $metadata)), row(['old_name' => 'test value'])),
            flow_context(config()),
        );

        static::assertSame('test value', $outputRows->first()->get('new_name'));
        static::assertTrue($outputRows->schema()->get('new_name')->metadata()->isEqual($metadata));
    }

    public function test_renaming_to_same_name_returns_same_rows_instance(): void
    {
        $inputRows = rows(schema(string_schema('name')), row(['name' => 'value']));

        static::assertSame($inputRows, (new RenameEntryTransformer('name', 'name'))->transform(
            $inputRows,
            flow_context(config()),
        ));
    }
}
