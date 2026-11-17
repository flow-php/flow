<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\DSL\{data_frame, lit, ref};

$orders = data_frame()->read(from_csv('orders.csv'));

// $orders stays as it was, $paid is a new frame
$paid = $orders->filter(ref('status')->equals(lit('paid')));

$paid->write(to_json('paid.json'))->run();
$orders->write(to_json('all.json'))->run();
