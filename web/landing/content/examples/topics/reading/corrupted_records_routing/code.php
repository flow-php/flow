<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\JSON\{from_json_lines, to_json_lines};
use function Flow\ETL\DSL\{count, data_frame, float_schema, int_schema, overwrite, ref, schema, to_output};

require __DIR__ . '/vendor/autoload.php';

// one record with an extra field, one amount that is not a number
file_put_contents(__DIR__ . '/orders.csv', "id,amount\n1,10.5\n2,abc\n3,7,extra\n4,1.5\n");

data_frame()
    ->read(from_csv(__DIR__ . '/orders.csv')
        ->withSchema(schema(int_schema('id'), float_schema('amount')))
        ->withCorruptedRecords(to_json_lines(__DIR__ . '/rejects.jsonl')->saveMode(overwrite())))
    ->write(to_output())
    ->run();

data_frame()
    ->read(from_json_lines(__DIR__ . '/rejects.jsonl'))
    ->groupBy(ref('reason'))
    ->aggregate(count(ref('offset')))
    ->write(to_output(truncate: false))
    ->run();
