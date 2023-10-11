<?php

declare(strict_types=1);

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\when;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;

require __DIR__ . '/../../bootstrap.php';

$flow = (new Flow())
    ->read(From::sequence_number('number', 1, 100))
    ->collect()
    ->withEntry(
        'type',
        when(
            ref('number')->isOdd(),
            then: lit('odd'),
            else: lit('even')
        )
    )
    ->write(To::output(false));

if ('' !== \Phar::running(false)) {
    return $flow;
}

$flow->run();
