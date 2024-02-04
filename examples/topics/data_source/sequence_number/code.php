<?php

declare(strict_types=1);

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_sequence_number('id', start: 0, end: 1000, step: 100))
    ->write(to_output(false))
    ->run();
