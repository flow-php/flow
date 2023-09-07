<?php

declare(strict_types=1);

require 'phar://flow-php.phar/vendor/autoload.php';

use Flow\ETL\PipelineFactory;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\SingleCommandApplication;
use Symfony\Component\Console\Style\SymfonyStyle;

(new SingleCommandApplication())
    ->setName('Flow-PHP - Extract Transform Load - Data processing framework')
    ->setVersion(FlowVersion::getVersion())
    ->addArgument('input-file', InputArgument::REQUIRED, '')
    ->setCode(function (InputInterface $input, OutputInterface $output): int {
        try {
            /** @phpstan-ignore-next-line */
            $loader = new PipelineFactory((string) $input->getArgument('input-file'));
            $loader->run();
        } catch (\Exception $exception) {
            $style = new SymfonyStyle($input, $output);
            $style->error($exception->getMessage());

            return Command::FAILURE;
        }

        return Command::SUCCESS;
    })
    ->run();
