<?php

declare(strict_types=1);

use function Flow\ETL\DSL\col;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

$extractor = require __FLOW_DATA__ . '/extractor.php';

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;

(new Flow())
    ->read($extractor)
    ->rows(Transform::array_unpack('row'))
    ->drop(col('row'))
    ->mode(SaveMode::Overwrite)
    ->partitionBy('country_code', 't_shirt_color')
    ->write(CSV::to(__FLOW_OUTPUT__ . '/partitioned'))
    ->run();

$stopwatch->stop();

print "Total writing CSV: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";
