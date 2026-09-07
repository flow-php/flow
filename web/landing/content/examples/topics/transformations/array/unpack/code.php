<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_rows, int_entry, int_schema, json_entry, ref, row, rows, schema, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_rows(rows(
        row(int_entry('id', 1), json_entry('array', ['a' => 1, 'b' => 2, 'c' => 3])),
        row(int_entry('id', 2), json_entry('array', ['d' => 4, 'e' => 5, 'f' => 6])),
    )))
    ->withEntry('unpacked', ref('array')->unpack(schema(
        int_schema('a'),
        int_schema('b'),
        int_schema('c'),
    )))
    ->write(to_output(truncate: false))
    ->run();
