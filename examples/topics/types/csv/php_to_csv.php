<?php

declare(strict_types=1);

use function Flow\ETL\DSL\col;
use function Flow\ETL\DSL\ref;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\Flow;
use Flow\ETL\Monitoring\Memory\Consumption;

require __DIR__ . '/../../../bootstrap.php';

$extractor = require __FLOW_DATA__ . '/extractor.php';

$flow = (new Flow())
    ->read($extractor)
    ->write(CSV::to(__FLOW_OUTPUT__ . '/dataset.csv'));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$stopwatch = new Stopwatch();
$stopwatch->start();
$memory = new Consumption();
$memory->current();

$flow->run();

$memory->current();
$stopwatch->stop();

print "Memory consumption, max: {$memory->max()->inMb()}Mb\n";
print "Total writing CSV: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";
