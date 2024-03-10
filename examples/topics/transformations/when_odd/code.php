<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{df, from_sequence_number, lit, ref, to_output, when};

require __DIR__ . '/../../../autoload.php';

df()
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
    ->write(to_output(false))
    ->run();
