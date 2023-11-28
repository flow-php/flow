<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Flow;

require __DIR__ . '/../../bootstrap.php';

$flow = (new Flow())
    ->read(from_sequence_number('id', 0, 10))
    ->sortBy(ref('id')->desc())
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$flow->run();
