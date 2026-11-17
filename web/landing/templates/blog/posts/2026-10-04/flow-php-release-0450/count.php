<?php

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\data_frame;

// rows and bytes of the source, known before a single row is read
$statistics = from_parquet('orders.parquet')->statistics();

// answered from the file footer
$total = data_frame()->read(from_parquet('orders.parquet'))->count();
