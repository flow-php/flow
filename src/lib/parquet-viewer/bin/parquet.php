#!/usr/bin/env php
<?php

use Composer\InstalledVersions;
use Flow\ParquetViewer\Parquet;

(function () {
    \error_reporting(E_ALL);
    \ini_set('display_errors', 'stderr');
    \ini_set('memory_limit', -1);

    if (is_file($autoload = getcwd() . '/../../../vendor/autoload.php')) {
        require $autoload;
    } elseif (is_file($autoload = getcwd() . '/../vendor/autoload.php')) {
        require $autoload;
    } else {
        fwrite(STDERR,
            'You must set up the project dependencies, run the following commands:' . PHP_EOL .
            'curl -s http://getcomposer.org/installer | php' . PHP_EOL .
            'php composer.phar install' . PHP_EOL
        );
        exit(1);
    }

    $application = new Parquet("Flow PHP - Parquet Viewer", InstalledVersions::getRootPackage()['pretty_version']);
    $application->run();
})();