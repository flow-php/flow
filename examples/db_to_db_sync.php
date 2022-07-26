<?php declare(strict_types=1);

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\DbalQueryExtractor;
use Flow\ETL\Adapter\Doctrine\ParametersSet;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;

require __DIR__ . '/../vendor/autoload.php';

if (!\is_dir(__DIR__ . '/var')) {
    \mkdir(__DIR__ . '/var');
}

if (!\is_dir(__DIR__ . '/var/run/')) {
    \mkdir(__DIR__ . '/var/run/');
}

// target db connection
$dbConnection = require __DIR__ . '/db_clean.php';

// source db connection
print "Loading source data into postgresql...\n";
[$sourceDbConnection, $rows] = require __DIR__ . '/db_source.php';

$stopwatch = new Stopwatch();
$stopwatch->start();

$batchSize = 1000;
$params = \array_fill(0, (int) \ceil($rows / $batchSize), ['limit' => $batchSize, 'offset' => 0]);
\array_walk($params, function (&$value, $key) : void {
    $value['offset'] = $value['limit'] * $key;
});

print "Loading {$rows} rows into postgresql...\n";

$extractor = new DbalQueryExtractor(
    $sourceDbConnection,
    'SELECT * FROM source_dataset_table ORDER BY id LIMIT :limit OFFSET :offset',
    new ParametersSet(...$params)
);

(new Flow())
    ->read($extractor)
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->rows(Transform::to_integer('id'))
    ->rows(Transform::string_concat(['name', 'last_name'], ' ', 'name'))
    ->drop('last_name')
    ->load(DbalLoader::fromConnection($dbConnection, 'flow_dataset_table', 1000))
    ->run();

$stopwatch->stop();

print 'Flow PHP - Elapsed time: ' . $stopwatch->totalElapsedTime()->inSecondsPrecise() . "s \n";
$dbRows = \current($dbConnection->executeQuery('SELECT COUNT(*) FROM flow_dataset_table')->fetchNumeric());
print "Total inserted rows: {$dbRows}\n";
