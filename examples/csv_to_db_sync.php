
<?php

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

require __DIR__ . '/../vendor/autoload.php';

if (!\is_dir(__DIR__ . '/var')) {
    \mkdir(__DIR__ . '/var');
}

if (!\is_dir(__DIR__ . '/var/run/')) {
    \mkdir(__DIR__ . '/var/run/');
}

$dbConnection = require __DIR__ . '/db_clean.php';

$logger = new Logger('server');
$logger->pushHandler(new StreamHandler(__DIR__ . '/../var/logs/server.log', Logger::DEBUG));
$logger->pushHandler(new StreamHandler(__DIR__ . '/../var/logs/server_error.log', Logger::ERROR, false));

$stopwatch = new Stopwatch();
$stopwatch->start();

$csvFileSize = \round(\filesize(__DIR__ . '/output/dataset.csv') / 1024 / 1024);
print "Loading CSV {$csvFileSize}Mb file into postgresql...\n";

(new Flow())
    ->read(CSV::from($path = __DIR__ . '/output/dataset.csv', 10_000))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->rows(Transform::to_integer('id'))
    ->rows(Transform::string_concat(['name', 'last name'], ' ', 'name'))
    ->drop('last name')
    ->load(DbalLoader::fromConnection($dbConnection, 'flow_dataset_table', 1000))
    ->run();

$stopwatch->stop();

print 'Flow PHP - Elapsed time: ' . $stopwatch->totalElapsedTime()->inSecondsPrecise() . "s \n";
$dbRows = \current($dbConnection->executeQuery('SELECT COUNT(*) FROM flow_dataset_table')->fetchNumeric());
print "Total inserted rows: {$dbRows}\n";
