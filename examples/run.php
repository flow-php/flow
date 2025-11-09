#!/usr/bin/env php
<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Composer\Semver\Semver;
use Flow\ETL\Dataset\Statistics\HighResolutionTime;
use Symfony\Component\Console\Input\{ArgvInput, InputDefinition, InputOption};
use Symfony\Component\Console\Output\ConsoleOutput;
use Symfony\Component\Console\Style\SymfonyStyle;
use Symfony\Component\Finder\Finder;

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    print PHP_EOL . 'This script cannot be run in PHAR, please use CLI approach.' . PHP_EOL;

    exit(1);
}

if (false === in_array(PHP_SAPI, ['cli', 'phpdbg', 'embed'], true)) {
    print PHP_EOL . 'This script may only be invoked from a command line, got "' . PHP_SAPI . '"' . PHP_EOL;

    exit(1);
}

ini_set('memory_limit', -1);

$output = new ConsoleOutput();
$input = new ArgvInput(definition: new InputDefinition(
    [
        new InputOption(name: 'composer-update', shortcut: 'u', mode: InputOption::VALUE_NONE),
        new InputOption(name: 'composer-archive', shortcut: 'a', mode: InputOption::VALUE_NONE),
        new InputOption(name: 'topic', shortcut: 't', mode: InputOption::VALUE_REQUIRED),
        new InputOption(name: 'example', shortcut: 'e', mode: InputOption::VALUE_REQUIRED),
        new InputOption(name: 'option', shortcut: 'o', mode: InputOption::VALUE_REQUIRED),
    ]
));

$topic = $input->getOption('topic');
$example = $input->getOption('example');
$option = $input->getOption('option');

$path = __DIR__ . '/topics';

if ($topic) {
    $path .= '/' . $topic;
}

if ($example) {
    $path .= '/' . $example;
}

if ($option) {
    $path .= '/' . $option;
}

$finder = new Finder();
$finder->in($path)
    ->files()
    ->name('*.php');

$style = new SymfonyStyle($input, $output);
$style->setDecorated(true);

$style->title('Running Flow PHP Examples');

foreach ($finder as $file) {
    if ($file->getBasename() !== 'code.php') {
        continue;
    }

    if (\file_exists($skipPath = $file->getPath() . '/skip.txt')) {
        $style->warning("Skipping example, skip.txt detected: {$skipPath}");

        continue;
    }

    if (\file_exists($composerPath = $file->getPath() . '/composer.json')) {
        $constraints = json_decode(file_get_contents($composerPath), true)['require']['php'] ?? '^8.2';

        if (!Semver::satisfies(\PHP_VERSION, $constraints)) {
            $style->warning(
                sprintf("Skipping example, used PHP (%s) doesn't satisfy requirements: {$constraints}", \PHP_VERSION)
            );

            continue;
        }
    }

    $start = HighResolutionTime::now();

    $style->info("Running example: {$file->getRelativePathname()}");

    $style->note(($input->getOption('composer-update') ? 'Updating' : 'Installing') . ' composer dependencies');
    $composerProcess = new Symfony\Component\Process\Process(['composer', $input->getOption('composer-update') ? 'update' : 'install'], $file->getPath());
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

    if ($input->getOption('composer-archive')) {
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
