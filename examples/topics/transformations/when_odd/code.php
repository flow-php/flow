<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_sequence_number, lit, ref, to_stream, when};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_sequence_number('number', 1, 100))
    ->collect()
    ->withEntry(
        'type',
        when(
            ref('number')->isOdd(),
            then: lit('odd'),
            else: lit('even')
        )
    )
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
