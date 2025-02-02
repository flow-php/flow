#!/usr/bin/env php
<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use function Flow\Filesystem\DSL\{fstab, path, protocol};
use Flow\ETL\Dataset\Statistics\HighResolutionTime;
use Symfony\Component\Console\Input\{ArgvInput, InputDefinition, InputOption};
use Symfony\Component\Console\Output\ConsoleOutput;
use Symfony\Component\Console\Style\SymfonyStyle;
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

$finder = new Finder();
$finder->in(__DIR__ . '/topics')
    ->files()
    ->name('*.php');

$output = new ConsoleOutput();
$intput = new ArgvInput(definition: new InputDefinition(
    [
        new InputOption(name: 'composer-update', shortcut: 'u', mode: InputOption::VALUE_NONE),
        new InputOption(name: 'composer-archive', shortcut: 'a', mode: InputOption::VALUE_NONE),
    ]
));
$style = new SymfonyStyle($intput, $output);
$style->setDecorated(true);

$style->title('Cleaning Flow PHP Examples');

$fs = fstab()->for(protocol('file'));

foreach ($finder as $file) {

    if ($file->getBasename() !== 'code.php') {
        continue;
    }

    $start = HighResolutionTime::now();

    $style->info("Removing vendor and code archive: {$file->getRelativePathname()}");

    $vendorPath = path($file->getPath() . '/vendor');

    if ($fs->status($vendorPath)?->isDirectory()) {
        $fs->rm(path($file->getPath() . '/vendor'));
    }

    $archiveZip = path($file->getPath() . '/flow_php_example.zip');

    if ($fs->status($archiveZip)?->isFile()) {
        $fs->rm($archiveZip);
    }
}

$style->success('Vendor adn archive folders remove from all examples');
