<?php declare(strict_types=1);

use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;

require __DIR__ . '/../../vendor/autoload.php';

(new Flow())
    ->read(CSV::from(Path::realpath(__DIR__ . '/issue289.csv')))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->write(CSV::to(Path::realpath(__DIR__ . '/issue289_new.csv'), true, false, ',', "'"))
    ->run();
