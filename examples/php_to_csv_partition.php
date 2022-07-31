<?php

declare(strict_types=1);

use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;

require __DIR__ . '/../vendor/autoload.php';

$extractor = require __DIR__ . '/data/extractor.php';

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;

(new Flow())
    ->read($extractor)
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->mode(SaveMode::Overwrite)
    ->partitionBy('country_code', 't_shirt_color')
    ->write(CSV::to(__DIR__ . '/output/partitioned'))
    ->run();

$stopwatch->stop();

print "Total writing CSV: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";
