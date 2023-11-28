<?php declare(strict_types=1);

use function Flow\ETL\DSL\from_csv;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\to_json;

require __DIR__ . '/../../../bootstrap.php';

if (\file_exists(__FLOW_OUTPUT__ . '/dataset.json')) {
    \unlink(__FLOW_OUTPUT__ . '/dataset.json');
}

$df = read(from_csv(__FLOW_OUTPUT__ . '/dataset.csv'))->write(to_json(__FLOW_OUTPUT__ . '/dataset.json'));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
