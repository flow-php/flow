<?php

declare(strict_types=1);

use function Flow\ETL\DSL\ref;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Partitions;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

$flow = (new Flow())
    ->read(CSV::from(__FLOW_DATA__ . '/partitioned'))
    ->withEntry('unpacked', ref('row')->unpack())
    ->renameAll('unpacked.', '')
    ->drop('row')
    ->collect()
    ->filterPartitions(Partitions::only('t_shirt_color', 'green'))
    ->sortBy(ref('id'))
    ->write(To::output());

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$stopwatch = new Stopwatch();
$stopwatch->start();

print "Reading partitioned CSV dataset with partition filtering...\n";

$flow->run();

$stopwatch->stop();

print "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n";
