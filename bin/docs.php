#!/usr/bin/env php
<?php

declare(strict_types=1);

use Flow\Documentation\{FunctionCollector, FunctionsExtractor};
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface};
use Symfony\Component\Console\Output\OutputInterface;

require __DIR__ . '/../vendor/autoload.php';

if (false === in_array(PHP_SAPI, ['cli', 'phpdbg', 'embed'], true)) {
    print PHP_EOL . 'This app may only be invoked from a command line, got "' . PHP_SAPI . '"' . PHP_EOL;

    exit(1);
}

ini_set('memory_limit', -1);

$application = new Application('Flow-PHP - Documentation');

$application->add(new class extends Command {
    public function configure() : void
    {
        $this
            ->setName('dsl:dump')
            ->setDescription('Dump DSL into json file.')
            ->addOption('repository-root-path', null, InputArgument::OPTIONAL, 'Repository root path.', dirname(__DIR__) . '/')
            ->addArgument('output', InputArgument::REQUIRED, 'Where to dump dsl.');
    }

    public function execute(InputInterface $input, OutputInterface $output) : int
    {

        $paths = [
            __DIR__ . '/../src/core/etl/src/Flow/ETL/DSL/functions.php',
            __DIR__ . '/../src/adapter/etl-adapter-chartjs/src/Flow/ETL/Adapter/ChartJS/functions.php',
            __DIR__ . '/../src/adapter/etl-adapter-csv/src/Flow/ETL/Adapter/CSV/functions.php',
            __DIR__ . '/../src/adapter/etl-adapter-doctrine/src/Flow/ETL/Adapter/Doctrine/functions.php',
            __DIR__ . '/../src/adapter/etl-adapter-elasticsearch/src/Flow/ETL/Adapter/Elasticsearch/functions.php',
            __DIR__ . '/../src/adapter/etl-adapter-google-sheet/src/Flow/ETL/Adapter/GoogleSheet/functions.php',
            __DIR__ . '/../src/adapter/etl-adapter-json/src/Flow/ETL/Adapter/JSON/functions.php',
            __DIR__ . '/../src/adapter/etl-adapter-meilisearch/src/Flow/ETL/Adapter/Meilisearch/functions.php',
            __DIR__ . '/../src/adapter/etl-adapter-parquet/src/Flow/ETL/Adapter/Parquet/functions.php',
            __DIR__ . '/../src/adapter/etl-adapter-text/src/Flow/ETL/Adapter/Text/functions.php',
            __DIR__ . '/../src/adapter/etl-adapter-xml/src/Flow/ETL/Adapter/XML/functions.php',
            __DIR__ . '/../src/lib/filesystem/src/Flow/Filesystem/DSL/functions.php',
            __DIR__ . '/../src/bridge/filesystem/azure/src/Flow/Filesystem/Bridge/Azure/DSL/functions.php',
            __DIR__ . '/../src/bridge/filesystem/async-aws/src/Flow/Filesystem/Bridge/AsyncAWS/DSL/functions.php',
            __DIR__ . '/../src/lib/azure-sdk/src/Flow/Azure/SDK/DSL/functions.php',
        ];

        $extractor = new FunctionsExtractor(
            $input->getOption('repository-root-path'),
            new FunctionCollector()
        );

        $normalizedFunctions = [];

        foreach ($extractor->extract($paths) as $function) {
            $normalizedFunctions[] = $function->normalize();
        }

        \file_put_contents(__DIR__ . '/../' . \ltrim((string) $input->getArgument('output'), '/'), \json_encode($normalizedFunctions));

        return Command::SUCCESS;
    }
});

$application->run();
