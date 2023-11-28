<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_text;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

$flow = (new Flow())
    ->read(from_text(__FLOW_DATA__ . '/annual-enterprise-survey-2019-financial-year-provisional.csv'));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$csvFileSize = \round(\filesize(__FLOW_DATA__ . '/annual-enterprise-survey-2019-financial-year-provisional.csv') / 1024 / 1024);
print "Reading CSV {$csvFileSize}Mb file...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();

$flow->run();

$stopwatch->stop();

print "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n";
