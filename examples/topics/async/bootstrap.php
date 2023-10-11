<?php declare(strict_types=1);

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    print "This example cannot be run in PHAR, please use CLI approach.\n";

    exit(1);
}

require __DIR__ . '/../../bootstrap.php';

const __FLOW_AUTOLOAD__ =  __DIR__ . '/../../../vendor/autoload.php';

// library autoload for all dependencies
require __FLOW_AUTOLOAD__;
