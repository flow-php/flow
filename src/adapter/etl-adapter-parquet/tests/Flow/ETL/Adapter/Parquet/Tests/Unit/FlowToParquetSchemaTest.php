<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use function Flow\ETL\DSL\{bool_schema,
    datetime_schema,
    float_schema,
    integer_schema,
    json_schema,
    list_schema,
    map_schema,
    string_schema,
    structure_schema,
    type_integer,
    type_list,
    type_map,
    type_structure};
use function Flow\ETL\DSL\{schema, type_boolean, type_int, type_string};
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, NestedColumn};

final class FlowToParquetSchemaTest extends FlowTestCase
{
    public function test_convert_etl_entries_to_parquet_fields() : void
    {
        self::assertEquals(
            ParquetSchema::with(
                FlatColumn::int64('integer', ParquetSchema\Repetition::REQUIRED),
                FlatColumn::boolean('boolean', ParquetSchema\Repetition::REQUIRED),
                FlatColumn::string('string', ParquetSchema\Repetition::REQUIRED),
                FlatColumn::float('float', ParquetSchema\Repetition::REQUIRED),
                FlatColumn::dateTime('datetime', ParquetSchema\Repetition::REQUIRED),
                FlatColumn::json('json', ParquetSchema\Repetition::REQUIRED),
                NestedColumn::list('list', ParquetSchema\ListElement::string(true), ParquetSchema\Repetition::REQUIRED),
                NestedColumn::list('list_of_structs', ParquetSchema\ListElement::structure(
                    [
                        FlatColumn::int64('integer', ParquetSchema\Repetition::REQUIRED),
                        FlatColumn::boolean('boolean', ParquetSchema\Repetition::REQUIRED),
                    ],
                    true
                ), ParquetSchema\Repetition::REQUIRED),
                NestedColumn::struct('structure', [FlatColumn::string('a', ParquetSchema\Repetition::REQUIRED)], ParquetSchema\Repetition::REQUIRED),
                NestedColumn::map('map', ParquetSchema\MapKey::string(), ParquetSchema\MapValue::int64(true), ParquetSchema\Repetition::REQUIRED),
            ),
            (new SchemaConverter())->toParquet(
                schema(
                    integer_schema('integer'),
                    bool_schema('boolean'),
                    string_schema('string'),
                    float_schema('float'),
                    datetime_schema('datetime'),
                    json_schema('json'),
                    list_schema('list', type_list(type_string())),
                    list_schema(
                        'list_of_structs',
                        type_list(
                            type_structure([
                                'integer' => type_int(),
                                'boolean' => type_boolean(),
                            ])
                        )
                    ),
                    structure_schema(
                        'structure',
                        type_structure([
                            'a' => type_string(),
                        ])
                    ),
                    map_schema('map', type_map(type_string(), type_integer()))
                )
            )
        );
    }
}
