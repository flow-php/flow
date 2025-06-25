<?php

declare(strict_types=1);

namespace Flow\ParquetViewer\Command;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{df, to_output};
use function Flow\Types\DSL\{type_list, type_string};
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Reader;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'read:data', description: 'Read data from parquet file')]
final class ReadDataCommand extends Command
{
    protected function configure() : void
    {
        $this
            ->addArgument('file', InputArgument::REQUIRED, 'path to parquet file')
            ->addOption('columns', 'c', InputOption::VALUE_OPTIONAL | InputOption::VALUE_IS_ARRAY, 'columns to read')
            ->addOption('limit', 'l', InputOption::VALUE_OPTIONAL, 'limit number of rows to read', 10)
            ->addOption('batch-size', 'b', InputOption::VALUE_OPTIONAL, 'batch size', 1000)
            ->addOption('truncate', 't', InputOption::VALUE_OPTIONAL, 'Truncate values in cells to given length, use empty to not truncate the output', 20);
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);
        $filePath = $input->getArgument('file');
        $filePathString = \is_string($filePath) ? $filePath : '';

        if (!\file_exists($filePathString)) {
            $style->error(\sprintf('File "%s" does not exist', $filePathString));

            return Command::FAILURE;
        }
        $reader = new Reader();
        $parquetFile = $reader->read($filePathString);

        try {
            $parquetFile->metadata();
        } catch (InvalidArgumentException) {
            $style->error(\sprintf('File "%s" is not a valid parquet file', $filePathString));

            return Command::FAILURE;
        }

        $batchSizeOption = $input->getOption('batch-size');
        $batchSize = \is_numeric($batchSizeOption) ? (int) $batchSizeOption : 1000;

        if ($batchSize < 1) {
            $style->error('Batch size must be positive number, got: ' . $batchSize);

            return Command::FAILURE;
        }

        $limitOption = $input->getOption('limit');
        $limit = \is_numeric($limitOption) ? (int) $limitOption : 0;
        $columns = type_list(type_string())->assert($input->getOption('columns'));
        $truncateOption = $input->getOption('truncate');
        $truncate = $truncateOption && \is_numeric($truncateOption) ? (int) $truncateOption : false;

        \ob_start();

        df()
            ->read(from_parquet($filePathString, $columns))
            ->limit($limit)
            ->batchSize($batchSize)
            ->write(to_output($truncate))
            ->run();

        $output->write(\ob_get_clean() ?: '');

        return Command::SUCCESS;
    }
}
