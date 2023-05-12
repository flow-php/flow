<?php declare(strict_types=1);

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\CSV;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;

require __DIR__ . '/../../vendor/autoload.php';

(new Flow())
    ->read(CSV::from(Path::realpath(__DIR__ . '/issue289.csv')))
    ->withEntry('unpacked', ref('row')->unpack())
    ->renameAll('unpacked.', '')
    ->drop('row')
    ->write(CSV::to(Path::realpath(__DIR__ . '/issue289_new.csv'), true, ',', "'"))
    ->run();
