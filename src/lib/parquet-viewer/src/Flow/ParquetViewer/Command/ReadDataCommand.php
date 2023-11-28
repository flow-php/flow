<?php declare(strict_types=1);

namespace Flow\ParquetViewer\Command;

use function Flow\ETL\DSL\to_output;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\Flow;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Reader;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
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
            ->addOption('truncate', 't', InputOption::VALUE_OPTIONAL, 'Truncate values in cells to given length');
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);
        $filePath = $input->getArgument('file');

        if (!\file_exists($filePath)) {
            $style->error(\sprintf('File "%s" does not exist', $filePath));

            return Command::FAILURE;
        }
        $reader = new Reader();
        $parquetFile = $reader->read($filePath);

        try {
            $parquetFile->metadata();
        } catch (InvalidArgumentException $e) {
            $style->error(\sprintf('File "%s" is not a valid parquet file', $filePath));

            return Command::FAILURE;
        }

        $batchSize = (int) $input->getOption('batch-size');

        if ($batchSize < 1) {
            $style->error('Batch size must be positive number, got: ' . $batchSize);

            return Command::FAILURE;
        }

        $limit = (int) $input->getOption('limit');
        $columns = $input->getOption('columns');
        $truncate = $input->getOption('truncate') ? (int) $input->getOption('truncate') : false;

        \ob_start();

        (new Flow())
            ->read(Parquet::from($filePath, $columns))
            ->limit($limit)
            ->batchSize($batchSize)
            ->write(to_output($truncate))
            ->run();

        $output->write(\ob_get_clean() ?: '');

        return Command::SUCCESS;
    }
}
