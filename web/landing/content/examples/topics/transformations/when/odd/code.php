<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_sequence_number, lit, ref, to_output, when};

require __DIR__ . '/vendor/autoload.php';

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
    ->write(to_output(truncate: false))
    ->run();
