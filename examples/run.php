#!/usr/bin/env php
<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

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

$output = new ConsoleOutput();
$intput = new ArgvInput(definition: new InputDefinition(
    [
        new InputOption(name: 'composer-update', shortcut: 'u', mode: InputOption::VALUE_NONE),
        new InputOption(name: 'composer-archive', shortcut: 'a', mode: InputOption::VALUE_NONE),
        new InputOption(name: 'topic', shortcut: 't', mode: InputOption::VALUE_REQUIRED),
        new InputOption(name: 'example', shortcut: 'e', mode: InputOption::VALUE_REQUIRED),
    ]
));

$topic = $intput->getOption('topic');
$example = $intput->getOption('example');

$path = __DIR__ . '/topics';

if ($topic) {
    $path .= '/' . $topic;
}

if ($example) {
    $path .= '/' . $example;
}

$finder = new Finder();
$finder->in($path)
    ->files()
    ->name('*.php');

$style = new SymfonyStyle($intput, $output);
$style->setDecorated(true);

$style->title('Running Flow PHP Examples');

foreach ($finder as $file) {

    if ($file->getBasename() !== 'code.php') {
        continue;
    }

    $start = HighResolutionTime::now();

    $style->info("Running example: {$file->getRelativePathname()}");

    $style->note(($intput->getOption('composer-update') ? 'Updating' : 'Installing') . ' composer dependencies');
    $composerProcess = new Symfony\Component\Process\Process(['composer', $intput->getOption('composer-update') ? 'update' : 'install'], $file->getPath());
    $composerProcess->run();
    $style->info('Composer install finished');

    if (!$composerProcess->isSuccessful()) {
        $style->error("Composer install failed: {$file->getPath()}");
        $style->error("Details: {$composerProcess->getErrorOutput()}");

        exit(1);
    }

    $codeProcess = new Symfony\Component\Process\Process(['php', $file->getRealPath()]);
    $codeProcess->run();

    if (!$codeProcess->isSuccessful()) {
        $style->error("Example failed: {$file->getPath()}");
        $style->error("Details: {$codeProcess->getOutput()}");

        exit(1);
    }
    $end = HighResolutionTime::now();

    $style->success('Example finished in ' . $start->diff($end)->toSeconds() . ' seconds');

    if ($intput->getOption('composer-archive')) {
        $style->note('Generating composer archive');
        $composerProcess = new Symfony\Component\Process\Process(['composer', 'archive', '--format', 'zip', '--file', 'flow_php_example'], $file->getPath());
        $composerProcess->run();

        if (!$composerProcess->isSuccessful()) {
            $style->error("Composer archive failed: {$file->getPath()}");
            $style->error("Details: {$composerProcess->getErrorOutput()}");

            exit(1);
        }

        $style->info('Composer archive generated');
    }
}
