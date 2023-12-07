<?php

declare(strict_types=1);

use Flow\ETL\DataFrame;

require __DIR__ . '/../../bootstrap.php';

$df = DataFrame::fromJson(\file_get_contents(__DIR__ . '/json/pipeline.json'));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

print "Reading XML dataset...\n";

$df->run();
