<?php declare(strict_types=1);

use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Json;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

if (\file_exists(__FLOW_OUTPUT__ . '/dataset.json')) {
    \unlink(__FLOW_OUTPUT__ . '/dataset.json');
}

$flow = (new Flow())
    ->read(CSV::from(__FLOW_OUTPUT__ . '/dataset.csv'))
    ->write(Json::to(__FLOW_OUTPUT__ . '/dataset.json'));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$flow->run();
