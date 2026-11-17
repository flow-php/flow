<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\JSON\{to_json, to_json_lines};
use function Flow\ETL\DSL\{data_frame, float_schema, int_schema, schema};

data_frame()
    ->read(from_csv('orders.csv')
        ->withSchema(schema(int_schema('id'), float_schema('amount')))
        ->withCorruptedRecords(to_json_lines('rejects/broken.jsonl')))
    ->write(to_json('clean.json'))
    ->run();
