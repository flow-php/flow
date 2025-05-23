<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use function Flow\ETL\DSL\{bool_entry,
    bool_schema,
    datetime_entry,
    datetime_schema,
    enum_entry,
    enum_schema,
    float_entry,
    float_schema,
    int_entry,
    int_schema,
    json_entry,
    json_schema,
    list_entry,
    list_schema,
    map_entry,
    map_schema,
    null_entry,
    row,
    rows,
    schema,
    string_schema,
    struct_entry,
    structure_schema,
    uuid_entry,
    uuid_schema,
    xml_entry,
    xml_schema};
use function Flow\Types\DSL\{type_datetime, type_float, type_integer, type_list, type_map, type_string, type_structure};
use Flow\ETL\Adapter\Parquet\RowsNormalizer;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;

final class RowsNormalizerTest extends FlowTestCase
{
    public function test_normalization_nullable_entries() : void
    {
        $rows = rows(
            row(
                int_entry('int', null),
                float_entry('float', null),
                bool_entry('bool', null),
                datetime_entry('datetime', null),
                null_entry('null'),
                uuid_entry('uuid', null),
                json_entry('json', null),
                list_entry('list', null, type_list(type_integer())),
                list_entry('list_of_datetimes', null, type_list(type_datetime())),
                map_entry(
                    'map',
                    null,
                    type_map(type_integer(), type_string())
                ),
                struct_entry(
                    'struct',
                    null,
                    type_structure([
                        'street' => type_string(),
                        'city' => type_string(),
                        'zip' => type_string(),
                        'country' => type_string(),
                        'location' => type_structure([
                            'lat' => type_float(),
                            'lon' => type_float(),
                        ]),
                    ]),
                ),
                enum_entry('enum', null),
                xml_entry('xml', null),
            )
        );
        $schema = schema(
            int_schema('int', true),
            float_schema('float', true),
            bool_schema('bool', true),
            datetime_schema('datetime', true),
            string_schema('null', nullable: true),
            uuid_schema('uuid', true),
            json_schema('json', true),
            list_schema('list', type_list(type_integer()), true),
            list_schema('list_of_datetimes', type_list(type_datetime()), true),
            map_schema('map', type_map(type_integer(), type_string()), true),
            structure_schema(
                'struct',
                type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                    'zip' => type_string(),
                    'country' => type_string(),
                    'location' => type_structure([
                        'lat' => type_float(),
                        'lon' => type_float(),
                    ]),
                ]),
                true
            ),
            enum_schema('enum', BackedStringEnum::class, true),
            xml_schema('xml', true),
        );

        self::assertEquals(
            [
                [
                    'int' => null,
                    'float' => null,
                    'bool' => null,
                    'datetime' => null,
                    'null' => null,
                    'uuid' => null,
                    'json' => null,
                    'list' => null,
                    'list_of_datetimes' => null,
                    'map' => null,
                    'struct' => null,
                    'enum' => null,
                    'xml' => null,
                ],
            ],
            (new RowsNormalizer())->normalize($rows, $schema)
        );
    }
}
