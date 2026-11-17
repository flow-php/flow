<?php

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{data_frame, lit, ref, to_output};

data_frame()
    ->read(from_parquet(__DIR__ . '/orders/*.parquet'))
    ->filter(ref('id')->greaterThan(lit(1000))->and(ref('status')->equals(lit('paid'))))
    ->limit(10)
    ->write(to_output())
    ->run();
