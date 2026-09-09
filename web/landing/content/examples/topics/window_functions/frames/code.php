<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{average, current_row, data_frame, from_array, int_schema, preceding, ref, schema, str_schema, sum, to_output, unbounded_preceding, window};

require __DIR__ . '/vendor/autoload.php';

// the frame decides how many rows the aggregate sees
data_frame()
    ->read(from_array([
        ['day' => '2024-01-01', 'sales' => 10],
        ['day' => '2024-01-02', 'sales' => 20],
        ['day' => '2024-01-03', 'sales' => 30],
        ['day' => '2024-01-04', 'sales' => 40],
    ], schema(str_schema('day'), int_schema('sales'))))
    // everything from the start of the partition up to this row
    ->withEntry('running_total', sum(ref('sales'))->over(
        window()->orderBy(ref('day'))->rowsBetween(unbounded_preceding(), current_row()),
    ))
    // this row and the one before it
    ->withEntry('moving_average', average(ref('sales'))->over(
        window()->orderBy(ref('day'))->rowsBetween(preceding(1), current_row()),
    ))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
