#!/usr/bin/env php
<?php declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Symfony\Component\Finder\Finder;

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    print PHP_EOL . 'This script cannot be run in PHAR, please use CLI approach.' . PHP_EOL;

    exit(1);
}

if (false === \in_array(PHP_SAPI, ['cli', 'phpdbg', 'embed'], true)) {
    print PHP_EOL . 'This script may only be invoked from a command line, got "' . PHP_SAPI . '"' . PHP_EOL;

    exit(1);
}

\ini_set('memory_limit', -1);

print "Running all available examples.\n";

$finder = new Finder();
$finder->in(__DIR__ . '/topics')
    ->files()
    ->name('*.php');

foreach ($finder as $file) {
    print "\nExample: {$file->getRelativePathname()}\n";

    try {
        include $file->getRealPath();
    } catch (Exception $e) {
        print "Example failed: {$e->getMessage()}\n";

        exit(1);
    }
}
