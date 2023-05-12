<?php

declare(strict_types=1);

use function Flow\ETL\DSL\col;
use function Flow\ETL\DSL\ref;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Json;
use Flow\ETL\Flow;
use Flow\ETL\Monitoring\Memory\Consumption;

require __DIR__ . '/../../../bootstrap.php';

$extractor = require __FLOW_DATA__ . '/extractor.php';

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;
$memory = new Consumption();
$memory->current();

(new Flow())
    ->read($extractor)
    ->withEntry('unpacked', ref('row')->unpack())
    ->renameAll('unpacked.', '')
    ->drop(col('row'))
    ->write(CSV::to(__FLOW_OUTPUT__ . '/dataset.csv'))
    ->write(Json::to(__FLOW_OUTPUT__ . '/dataset.json'))
    ->run();

$memory->current();
$stopwatch->stop();

print "Memory consumption, max: {$memory->max()->inMb()}Mb\n";
print "Total writing CSV: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";
