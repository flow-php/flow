<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;

final class ParquetToFlowSchemaTest extends FlowTestCase
{
    public function test_converting_flat_fields_to_flow_schema(): void
    {
        $converted = new SchemaConverter();

        $flowSchema = $converted->toFlow(Schema::with(
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

        static::assertEquals(
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
            $flowSchema,
        );
    }

    public function test_converting_list_to_flow_schema(): void
    {
        $converted = new SchemaConverter();

        $flowSchema = $converted->toFlow(Schema::with(NestedColumn::list('list', ListElement::string())));

        static::assertEquals(
            \Flow\ETL\DSL\schema(list_schema('list', type_list(type_optional(type_string())), true)),
            $flowSchema,
        );
    }

    public function test_converting_map_to_flow_schema(): void
    {
        $converted = new SchemaConverter();

        $flowSchema = $converted->toFlow(Schema::with(NestedColumn::map('map', MapKey::string(), MapValue::int64())));

        static::assertEquals(
            \Flow\ETL\DSL\schema(map_schema('map', type_map(type_string(), type_optional(type_integer())), true)),
            $flowSchema,
        );
    }

    public function test_converting_struct_to_flow_schema(): void
    {
        $converted = new SchemaConverter();

        $flowSchema = $converted->toFlow(Schema::with(NestedColumn::struct('struct', [
            FlatColumn::uuid('uuid'),
            FlatColumn::string('name'),
            FlatColumn::boolean('active'),
        ])));

        static::assertEquals(
            \Flow\ETL\DSL\schema(structure_schema(
                'struct',
                type_structure([
                    'uuid' => type_optional(type_uuid()),
                    'name' => type_optional(type_string()),
                    'active' => type_optional(type_boolean()),
                ]),
                true,
            )),
            $flowSchema,
        );
    }
}
