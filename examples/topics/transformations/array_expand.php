<?php

declare(strict_types=1);

use function Flow\ETL\DSL\array_expand;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$df = read(
    from_rows(new Rows(
        Row::with(Entry::int('id', 1), Entry::array('array', ['a' => 1, 'b' => 2, 'c' => 3])),
    ))
)
    ->write(to_output(false))
    ->withEntry('expanded', array_expand(ref('array')))
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
