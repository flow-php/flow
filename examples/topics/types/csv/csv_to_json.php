<?php declare(strict_types=1);

use function Flow\ETL\DSL\col;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Json;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

if (\file_exists(__FLOW_OUTPUT__ . '/dataset.json')) {
    \unlink(__FLOW_OUTPUT__ . '/dataset.json');
}

return (new Flow())
    ->read(CSV::from(__FLOW_OUTPUT__ . '/dataset.csv'))
    ->withEntry('unpacked', ref('row')->unpack())
    ->renameAll('unpacked.', '')
    ->drop(col('row'))
    ->write(Json::to(__FLOW_OUTPUT__ . '/dataset.json'));
