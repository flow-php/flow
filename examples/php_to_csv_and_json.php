<?php

declare(strict_types=1);

use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Json;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\Monitoring\Memory\Consumption;

require __DIR__ . '/../vendor/autoload.php';

$extractor = require __DIR__ . '/data/extractor.php';

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;
$memory = new Consumption();
$memory->current();

(new Flow())
    ->read($extractor)
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->write(CSV::to(__DIR__ . '/output/dataset.csv'))
    ->write(Json::to(__DIR__ . '/output/dataset.json'))
    ->run();

$memory->current();
$stopwatch->stop();

print "Memory consumption, max: {$memory->max()->inMb()}Mb\n";
print "Total writing CSV: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";
