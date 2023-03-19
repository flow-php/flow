<?php declare(strict_types=1);

use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Json;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\Monitoring\Memory\Consumption;
use Flow\ETL\Rows;
use function Flow\ETL\DSL\col;

require __DIR__ . '/../../../bootstrap.php';

if (\file_exists(__FLOW_OUTPUT__ . '/dataset.json')) {
    \unlink(__FLOW_OUTPUT__ . '/dataset.json');
}

$csvFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset.csv') / 1024 / 1024);
print "Converting CSV {$csvFileSize}Mb file into json...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;
$memory = new Consumption();
$memory->current();

(new Flow())
    ->read(CSV::from(__FLOW_OUTPUT__ . '/dataset.csv'))
    ->rows(Transform::array_unpack('row'))
    ->drop(col('row'))
    ->write(To::callback(static function (Rows $rows) use (&$total, $memory) : void {
        $total += $rows->count();

        $memory->current();
    }))
    ->write(Json::to(__FLOW_OUTPUT__ . '/dataset.json'))
    ->run();

$memory->current();
$stopwatch->stop();

print "Memory consumption, max: {$memory->max()->inMb()}Mb\n";
print "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n";

$jsonFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset.json') / 1024 / 1024);
print "Output JSON file size: {$jsonFileSize}Mb\n";
