<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_csv;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

$flow = (new Flow())
    ->read(from_csv(__FLOW_DATA__ . '/partitioned'))
    ->collect()
    ->filterPartitions(ref('t_shirt_color')->equals(lit('green')))
    ->sortBy(ref('id'))
    ->write(to_output());

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$stopwatch = new Stopwatch();
$stopwatch->start();

print "Reading partitioned CSV dataset with partition filtering...\n";

$flow->run();

$stopwatch->stop();

print "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n";
