<?php declare(strict_types=1);

if ('' !== \Phar::running(false)) {
    print 'This example cannot be run in PHAR, please use CLI approach.';

    exit(1);
}

require __DIR__ . '/../../bootstrap.php';

const __FLOW_AUTOLOAD__ =  __DIR__ . '/../vendor/autoload.php';

// library autoload for all dependencies
require __FLOW_AUTOLOAD__;
