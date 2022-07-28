<?php

declare(strict_types=1);

use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\Monitoring\Memory\Consumption;
use Flow\ETL\Rows;

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
    ->write(To::callback(static function (Rows $rows) use (&$total, $memory) : void {
        $total += $rows->count();

        $memory->current();
    }))
    ->partitionBy('country_code', 'color')
    ->write(CSV::to(__DIR__ . '/output/partitioned'))
    ->run();

$memory->current();
$stopwatch->stop();

print "Memory consumption, max: {$memory->max()->inMb()}Mb\n";
print "Total writing CSV: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";
