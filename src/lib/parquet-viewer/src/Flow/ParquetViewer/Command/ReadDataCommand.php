<?php

declare(strict_types=1);

namespace Flow\ParquetViewer\Command;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Reader;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function file_exists;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\to_output;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_scalar;
use function Flow\Types\DSL\type_string;
use function is_numeric;
use function ob_get_clean;
use function ob_start;
use function sprintf;

#[AsCommand(name: 'read:data', description: 'Read data from parquet file')]
final class ReadDataCommand extends Command
{
    protected function configure(): void
    {
        $this
            ->addArgument('file', InputArgument::REQUIRED, 'path to parquet file')
            ->addOption('columns', 'c', InputOption::VALUE_OPTIONAL | InputOption::VALUE_IS_ARRAY, 'columns to read')
            ->addOption('limit', 'l', InputOption::VALUE_OPTIONAL, 'limit number of rows to read', 10)
            ->addOption('batch-size', 'b', InputOption::VALUE_OPTIONAL, 'batch size', 1000)
            ->addOption(
                'truncate',
                't',
                InputOption::VALUE_OPTIONAL,
                'Truncate values in cells to given length, use empty to not truncate the output',
                20,
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $style = new SymfonyStyle($input, $output);
        $filePath = type_string()->assert($input->getArgument('file'));

        if (!file_exists($filePath)) {
            $style->error(sprintf('File "%s" does not exist', $filePath));

            return Command::FAILURE;
        }
        $reader = new Reader();
        $parquetFile = $reader->read($filePath);

        try {
            $parquetFile->metadata();
        } catch (InvalidArgumentException) {
            $style->error(sprintf('File "%s" is not a valid parquet file', $filePath));

            return Command::FAILURE;
        }

        $batchSizeOption = type_optional(type_scalar())->assert($input->getOption('batch-size'));
        $batchSize = is_numeric($batchSizeOption) ? (int) $batchSizeOption : 1000;

        if ($batchSize < 1) {
            $style->error('Batch size must be positive number, got: ' . $batchSize);

            return Command::FAILURE;
        }

        $limitOption = type_optional(type_scalar())->assert($input->getOption('limit'));
        $limit = is_numeric($limitOption) ? (int) $limitOption : 0;
        $columns = type_list(type_string())->assert($input->getOption('columns'));
        $truncateOption = type_optional(type_scalar())->assert($input->getOption('truncate'));
        $truncate = is_numeric($truncateOption) && $truncateOption ? (int) $truncateOption : false;

        ob_start();

        df()
            ->read(from_parquet($filePath, $columns))
            ->limit($limit)
            ->batchSize($batchSize)
            ->write(to_output($truncate))
            ->run();

        $output->write(ob_get_clean() ?: '');

        return Command::SUCCESS;
    }
}
