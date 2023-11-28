<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use function Flow\ETL\DSL\when;
use Flow\ETL\Flow;

require __DIR__ . '/../../bootstrap.php';

$flow = (new Flow())
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
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$flow->run();
