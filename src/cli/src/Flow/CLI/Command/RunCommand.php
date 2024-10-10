<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use Flow\ETL\Exception\{Exception, InvalidFileFormatException};
use Flow\ETL\{DataFrame, PipelineFactory};
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

final class RunCommand extends Command
{
    public function configure() : void
    {
        $this
            ->setName('run')
            ->setDescription('Execute ETL pipeline from a php/json file.')
            ->addArgument('input-file', InputArgument::REQUIRED, 'Path to a php/json with DataFrame definition.');
    }

    public function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);

        try {
            try {
                $dataFrame = new PipelineFactory((string) $input->getArgument('input-file'));
                $dataFrame->run();
            } catch (InvalidFileFormatException $notPhpFileException) {
                $jsonPath = \file_get_contents((string) $input->getArgument('input-file'));

                if ($jsonPath === false) {
                    $style->error('Cannot read file: ' . $input->getArgument('input-file'));

                    return Command::FAILURE;
                }

                $dataFrame = DataFrame::fromJson($jsonPath);
                $dataFrame->run();
            }

        } catch (Exception $exception) {

            $style->error($exception->getMessage());

            return Command::FAILURE;
        }

        return Command::SUCCESS;
    }
}
