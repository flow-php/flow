<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\object_schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_schema;
use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_map;
use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\type_structure;
use function Flow\ETL\DSL\type_uuid;
use function Flow\ETL\DSL\uuid_schema;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use PHPUnit\Framework\TestCase;

final class ParquetToFlowSchemaTest extends TestCase
{
    public function test_converting_flat_fields_to_flow_schema() : void
    {
        $converted = new SchemaConverter();

        $flowSchema = $converted->fromParquet(Schema::with(
            Schema\FlatColumn::int32('int32'),
            Schema\FlatColumn::int64('int64'),
            Schema\FlatColumn::string('string'),
            Schema\FlatColumn::float('float'),
            Schema\FlatColumn::double('double'),
            Schema\FlatColumn::decimal('decimal'),
            Schema\FlatColumn::boolean('boolean'),
            Schema\FlatColumn::date('date'),
            Schema\FlatColumn::time('time'),
            Schema\FlatColumn::dateTime('datetime'),
            Schema\FlatColumn::uuid('uuid'),
            Schema\FlatColumn::json('json'),
        ));

        $this->assertEquals(
            \Flow\ETL\DSL\schema(
                int_schema('int32', true),
                int_schema('int64', true),
                str_schema('string', true),
                float_schema('float', true),
                float_schema('double', true),
                float_schema('decimal', true),
                bool_schema('boolean', true),
                datetime_schema('date', true),
                object_schema('time', type_object(\DateInterval::class, true)),
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
            Schema\NestedColumn::list('list', Schema\ListElement::string()),
        ));

        $this->assertEquals(
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
            Schema\NestedColumn::map('map', MapKey::string(), MapValue::int64()),
        ));

        $this->assertEquals(
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
            Schema\NestedColumn::struct(
                'struct',
                [
                    Schema\FlatColumn::uuid('uuid'),
                    Schema\FlatColumn::string('name'),
                    Schema\FlatColumn::boolean('active'),
                ]
            ),
        ));

        $this->assertEquals(
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
