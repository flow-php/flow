<?php declare(strict_types=1);

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    print "This example cannot be run in PHAR, please use CLI approach.\n";

    exit(1);
}

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\local_files;
use function Flow\ETL\DSL\path_real;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../../vendor/autoload.php';

data_frame()
    ->read(local_files(path_real(__DIR__ . '/../'), true))
    ->collect()
    ->select('file_name', 'base_name', 'is_dir', 'size')
    ->write(to_output(false))
    ->run();
