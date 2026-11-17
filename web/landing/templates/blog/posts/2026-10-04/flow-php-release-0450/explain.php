<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\DSL\{data_frame, ref};

$dataFrame = data_frame()
    ->read(from_csv('/data/orders.csv'))
    ->filter(ref('email')->isNotNull())
    ->write(to_json('out.json'))
    ->select('id', 'email');

echo $dataFrame->explain()->toString();
