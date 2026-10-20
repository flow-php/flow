#!/usr/bin/env php
<?php

declare(strict_types=1);

use Symfony\Component\Console\Application;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Process\ExecutableFinder;
use Symfony\Component\Process\Process;

use function Flow\Types\DSL\type_string;

require __DIR__ . '/../vendor/autoload.php';

$application = new Application('Flow-PHP - Thrift');

$application->addCommand(new class extends Command {
    public function configure(): void
    {
        $this
            ->setName('parquet')
            ->setDescription('Regenerates the Parquet ThriftModel classes from parquet.thrift.')
            ->addOption(
                'thrift',
                null,
                InputOption::VALUE_REQUIRED,
                'Thrift compiler binary, found in PATH when omitted',
            );
    }

    public function execute(InputInterface $input, OutputInterface $output): int
    {
        $source = __DIR__ . '/../src/lib/parquet/src';
        $idl = $source . '/Flow/Parquet/Resources/Thrift/parquet.thrift';
        $models = $source . '/Flow/Parquet/ThriftModel';

        $thrift = $input->getOption('thrift') === null
            ? (new ExecutableFinder())->find('thrift')
            : type_string()->assert($input->getOption('thrift'));

        if (!is_string($thrift) || !is_executable($thrift)) {
            $output->writeln('<error>Thrift compiler not found, pass an executable with --thrift</error>');

            return Command::FAILURE;
        }

        if (!str_contains((string) file_get_contents($idl), 'namespace php Flow.Parquet.ThriftModel')) {
            $output->writeln('<error>Flow php namespace not found in thrift definition!</error>');

            return Command::FAILURE;
        }

        foreach (glob($models . '/*.php') ?: [] as $file) {
            unlink($file);
        }

        (new Process([$thrift, '--gen', 'php', '--out', $source, $idl]))->mustRun(
            static fn(string $type, string $buffer) => $output->write($buffer),
        );

        // the Thrift compiler types every field non-null; a field the IDL does not require is null when a file leaves it out
        foreach (glob($models . '/*.php') ?: [] as $file) {
            $class = 'Flow\\Parquet\\ThriftModel\\' . basename($file, '.php');

            if (!class_exists($class) || !property_exists($class, '_TSPEC')) {
                continue;
            }

            /** @var array<int, array{var: string, isRequired: bool}> $spec */
            $spec = (new ReflectionProperty($class, '_TSPEC'))->getValue();
            $model = (string) file_get_contents($file);
            $rewritten = $model;

            foreach ($spec as $field) {
                if ($field['isRequired']) {
                    continue;
                }

                $rewritten = (string) preg_replace(
                    '/(\* @var )(?!null\|)([^\n]+)(\n\s*\*\/\n\s*public \$'
                    . preg_quote($field['var'], '/')
                    . ' = null;)/',
                    '$1null|$2$3',
                    $rewritten,
                );
            }

            if ($rewritten !== $model) {
                file_put_contents($file, $rewritten);
            }
        }

        return Command::SUCCESS;
    }
});

$application->run();
