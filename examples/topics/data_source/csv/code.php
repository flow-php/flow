<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_csv(
        __DIR__ . '/input/dataset.csv',
        with_header: true,
        empty_to_null: true,
        delimiter: ',',
        enclosure: '"',
        escape: '\\',
        characters_read_in_line: 1000
    ))
    ->write(to_output(false))
    ->run();
