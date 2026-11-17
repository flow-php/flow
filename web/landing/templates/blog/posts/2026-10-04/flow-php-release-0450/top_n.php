<?php

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{data_frame, ref, to_output};

// the 10 biggest orders: Flow keeps only the top 10 rows while reading,
// it does not sort the whole file first
data_frame()
    ->read(from_parquet('orders.parquet'))
    ->sortBy(ref('amount')->desc())
    ->limit(10)
    ->write(to_output())
    ->run();
