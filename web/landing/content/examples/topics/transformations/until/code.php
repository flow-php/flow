<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_sequence_number, lit, ref, to_output};

require __DIR__ . '/vendor/autoload.php';

// until() sends STOP to the extractor, so the source stops producing
echo "until(id < 5) on an unbounded source:\n";
data_frame()
    ->read(from_sequence_number('id', 1, 1_000_000))
    ->until(ref('id')->lessThan(lit(5)))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();

// filter() lets the source run to the end and discards as it goes
echo "\nfilter(id < 5) over a bounded one:\n";
data_frame()
    ->read(from_sequence_number('id', 1, 10))
    ->filter(ref('id')->lessThan(lit(5)))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
