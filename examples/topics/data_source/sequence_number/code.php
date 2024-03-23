<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_sequence_number, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_sequence_number('id', start: 0, end: 1000, step: 100))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
