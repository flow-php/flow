<?php

declare(strict_types=1);

namespace Flow\ParquetViewer\Command;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Reader;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'read:ddl', description: 'Read DDL from parquet file')]
final class ReadDDLCommand extends Command
{
    protected function configure() : void
    {
        $this
            ->addArgument('file', InputArgument::REQUIRED, 'path to parquet file');
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
        } catch (InvalidArgumentException) {
            $style->error(\sprintf('File "%s" is not a valid parquet file', $filePath));

            return Command::FAILURE;
        }

        $style->title('Parquet file DDL');

        $style->writeln(\json_encode($parquetFile->metadata()->schema()->toDDL(), JSON_THROW_ON_ERROR | \JSON_PRETTY_PRINT));

        return Command::SUCCESS;
    }
}
