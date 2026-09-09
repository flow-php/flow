<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{col, data_frame, from_array, int_schema, pivot_values, schema, str_schema, sum, to_output};

require __DIR__ . '/vendor/autoload.php';

// the pivot columns are declared, so the plan knows them before a row is read
data_frame()
    ->read(from_array([
        ['region' => 'EU', 'product' => 'Laptop', 'sales' => 100],
        ['region' => 'EU', 'product' => 'Phone', 'sales' => 50],
        ['region' => 'US', 'product' => 'Laptop', 'sales' => 200],
        ['region' => 'US', 'product' => 'Phone', 'sales' => 75],
    ], schema(str_schema('region'), str_schema('product'), int_schema('sales'))))
    ->groupBy(col('region'))
    ->pivot(col('product'), pivot_values('Laptop', 'Phone'))
    ->aggregate(sum(col('sales')))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
