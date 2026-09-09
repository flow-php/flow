<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{
    bool_schema,
    data_frame,
    datetime_schema,
    float_schema,
    from_array,
    int_schema,
    json_schema,
    list_schema,
    map_schema,
    schema,
    schema_sort_by_type_and_name,
    str_schema,
    structure_schema,
    uuid_schema};
use function Flow\Types\DSL\{type_float, type_integer, type_list, type_map, type_string, type_structure};

require __DIR__ . '/vendor/autoload.php';

// column order is the schema's, so reordering means sorting the schema, not the rows
$schema = schema(
    int_schema('int_a'),
    int_schema('int_b'),
    float_schema('float_a'),
    float_schema('float_b'),
    bool_schema('bool'),
    bool_schema('bool_a'),
    bool_schema('bool_c'),
    datetime_schema('datetime_d'),
    datetime_schema('datetime_z'),
    str_schema('string_a'),
    str_schema('string_b'),
    uuid_schema('uuid'),
    json_schema('json'),
    list_schema('list', type_list(type_integer())),
    map_schema('map', type_map(type_integer(), type_string())),
    structure_schema('struct', type_structure([
        'street' => type_string(),
        'city' => type_string(),
        'zip' => type_string(),
        'country' => type_string(),
        'location' => type_structure([
            'lat' => type_float(),
            'lon' => type_float(),
        ]),
    ])),
);

data_frame()
    ->read(from_array([
        [
            'int_a' => 1,
            'int_b' => 1,
            'float_a' => 57291 / 100,
            'float_b' => 21021 / 100,
            'bool' => false,
            'bool_a' => false,
            'bool_c' => false,
            'datetime_d' => new DateTimeImmutable('2024-04-01 00:00:00'),
            'datetime_z' => new DateTimeImmutable('2024-04-01 00:00:00'),
            'string_a' => 'string',
            'string_b' => 'string',
            'uuid' => '06143adb-3009-45c8-a4f0-c7016f97cab7',
            'json' => ['id' => 1, 'status' => 'NEW'],
            'list' => [1, 2, 3],
            'map' => [0 => 'zero', 1 => 'one', 2 => 'two'],
            'struct' => [
                'street' => 'street',
                'city' => 'city',
                'zip' => 'zip',
                'country' => 'country',
                'location' => ['lat' => 1.5, 'lon' => 1.5],
            ],
        ],
    ], $schema->sort(schema_sort_by_type_and_name())))
    ->printSchema();
