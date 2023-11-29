<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\to_csv;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

$extractor = require __FLOW_DATA__ . '/extractor.php';

$flow = (new Flow())
    ->read($extractor)
    ->mode(SaveMode::Overwrite)
    ->partitionBy('country_code', 't_shirt_color')
    ->write(to_csv(__FLOW_OUTPUT__ . '/partitioned'));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$stopwatch = new Stopwatch();
$stopwatch->start();

$flow->run();

$stopwatch->stop();

print "Total writing CSV: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";
