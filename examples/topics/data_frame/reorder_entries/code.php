<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{array_entry,
    bool_entry,
    compare_entries_by_type_and_name,
    data_frame,
    datetime_entry,
    float_entry,
    from_rows,
    int_entry,
    json_entry,
    list_entry,
    map_entry,
    object_entry,
    row,
    rows,
    str_entry,
    struct_element,
    struct_entry,
    struct_type,
    to_output,
    to_stream,
    type_float,
    type_int,
    type_list,
    type_map,
    type_string,
    uuid_entry};

data_frame()
    ->read(from_rows(rows(
        row(
            int_entry('int_a', 1),
            int_entry('int_b', 1),
            float_entry('float_a', 57291 / 100),
            float_entry('float_b', 21021 / 100),
            bool_entry('bool', false),
            bool_entry('bool_a', false),
            bool_entry('bool_c', false),
            datetime_entry('datetime_d', new DateTimeImmutable('2024-04-01 00:00:00')),
            datetime_entry('datetime_z', new DateTimeImmutable('2024-04-01 00:00:00')),
            str_entry('string_a', 'string'),
            str_entry('string_b', 'string'),
            uuid_entry('uuid', '06143adb-3009-45c8-a4f0-c7016f97cab7'),
            json_entry('json', ['id' => 1, 'status' => 'NEW']),
            array_entry(
                'array',
                [
                    ['id' => 1, 'status' => 'NEW'],
                    ['id' => 2, 'status' => 'PENDING'],
                ]
            ),
            list_entry('list', [1, 2, 3], type_list(type_int())),
            map_entry('map', [0 => 'zero', 1 => 'one', 2 => 'two'], type_map(type_int(), type_string())),
            struct_entry(
                'struct',
                [
                    'street' => 'street',
                    'city' => 'city',
                    'zip' => 'zip',
                    'country' => 'country',
                    'location' => ['lat' => 1.5, 'lon' => 1.5],
                ],
                struct_type([
                    struct_element('street', type_string()),
                    struct_element('city', type_string()),
                    struct_element('zip', type_string()),
                    struct_element('country', type_string()),
                    struct_element(
                        'location',
                        struct_type([
                            struct_element('lat', type_float()),
                            struct_element('lon', type_float()),
                        ])
                    ),
                ]),
            ),
            object_entry('object', new ArrayIterator([1, 2, 3])),
        )
    )))
    ->reorderEntries(compare_entries_by_type_and_name())
    ->write(to_output(false))
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
