<?php

declare(strict_types=1);

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use function Flow\ETL\DSL\when;

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
