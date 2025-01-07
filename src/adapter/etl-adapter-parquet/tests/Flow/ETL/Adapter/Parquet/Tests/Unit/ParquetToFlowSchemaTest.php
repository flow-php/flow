<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use function Flow\ETL\DSL\{bool_schema,
    date_schema,
    datetime_schema,
    float_schema,
    int_schema,
    json_schema,
    list_schema,
    map_schema,
    str_schema,
    struct_element,
    struct_schema,
    time_schema,
    type_boolean,
    type_int,
    type_list,
    type_map,
    type_string,
    type_structure,
    type_uuid,
    uuid_schema};
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use Flow\Parquet\ParquetFile\Schema\{MapKey, MapValue};

final class ParquetToFlowSchemaTest extends FlowTestCase
{
    public function test_converting_flat_fields_to_flow_schema() : void
    {
        $converted = new SchemaConverter();

        $flowSchema = $converted->fromParquet(Schema::with(
            FlatColumn::int32('int32'),
            FlatColumn::int64('int64'),
            FlatColumn::string('string'),
            FlatColumn::float('float'),
            FlatColumn::double('double'),
            FlatColumn::decimal('decimal'),
            FlatColumn::boolean('boolean'),
            FlatColumn::date('date'),
            FlatColumn::time('time'),
            FlatColumn::dateTime('datetime'),
            FlatColumn::uuid('uuid'),
            FlatColumn::json('json'),
        ));

        self::assertEquals(
            \Flow\ETL\DSL\schema(
                int_schema('int32', true),
                int_schema('int64', true),
                str_schema('string', true),
                float_schema('float', true),
                float_schema('double', true),
                float_schema('decimal', true),
                bool_schema('boolean', true),
                date_schema('date', true),
                time_schema('time', true),
                datetime_schema('datetime', true),
                uuid_schema('uuid', true),
                json_schema('json', true),
            ),
            $flowSchema
        );
    }

    public function test_converting_list_to_flow_schema() : void
    {
        $converted = new SchemaConverter();

        $flowSchema = $converted->fromParquet(Schema::with(
            NestedColumn::list('list', ListElement::string()),
        ));

        self::assertEquals(
            \Flow\ETL\DSL\schema(
                list_schema('list', type_list(type_string(true), true))
            ),
            $flowSchema,
        );
    }

    public function test_converting_map_to_flow_schema() : void
    {
        $converted = new SchemaConverter();

        $flowSchema = $converted->fromParquet(Schema::with(
            NestedColumn::map('map', MapKey::string(), MapValue::int64()),
        ));

        self::assertEquals(
            \Flow\ETL\DSL\schema(
                map_schema('map', type_map(type_string(), type_int(true), true))
            ),
            $flowSchema,
        );
    }

    public function test_converting_struct_to_flow_schema() : void
    {
        $converted = new SchemaConverter();

        $flowSchema = $converted->fromParquet(Schema::with(
            NestedColumn::struct(
                'struct',
                [
                    FlatColumn::uuid('uuid'),
                    FlatColumn::string('name'),
                    FlatColumn::boolean('active'),
                ]
            ),
        ));

        self::assertEquals(
            \Flow\ETL\DSL\schema(
                struct_schema(
                    'struct',
                    type_structure(
                        [
                            struct_element('uuid', type_uuid(true)),
                            struct_element('name', type_string(true)),
                            struct_element('active', type_boolean(true)),
                        ],
                        true
                    ),
                )
            ),
            $flowSchema,
        );
    }
}
