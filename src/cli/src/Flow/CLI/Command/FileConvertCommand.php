<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use Flow\CLI\Arguments\FilePathArgument;
use Flow\CLI\Factory\ExtractorFactory;
use Flow\CLI\Factory\LoaderFactory;
use Flow\CLI\Options\ConfigOption;
use Flow\CLI\Options\CSVInputOptions;
use Flow\CLI\Options\CSVOutputOptions;
use Flow\CLI\Options\ExcelInputOptions;
use Flow\CLI\Options\FileFormat;
use Flow\CLI\Options\FileFormatOption;
use Flow\CLI\Options\JSONInputOptions;
use Flow\CLI\Options\JSONOutputOptions;
use Flow\CLI\Options\ParquetInputOptions;
use Flow\CLI\Options\SchemaInferenceOptions;
use Flow\CLI\Options\XMLInputOptions;
use Flow\CLI\Options\XMLOutputOptions;
use Flow\ETL\Runtime\Config;
use Flow\Filesystem\Path;
use RuntimeException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function Flow\CLI\option_bool;
use function Flow\CLI\option_int;
use function Flow\CLI\option_int_nullable;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\overwrite;
use function number_format;

#[AsCommand(name: 'file:convert', description: 'Convert data from one file format to another.', aliases: ['convert'])]
final class FileConvertCommand extends Command
{
    private const int DEFAULT_BATCH_SIZE = 100;

    private ?Config $flowConfig = null;

    private ?Path $inputFile = null;

    private ?FileFormat $inputFileFormat = null;

    private ?Path $outputFile = null;

    private ?FileFormat $outputFileFormat = null;

    public function configure(): void
    {
        $this
            ->addArgument(
                'input-file',
                InputArgument::REQUIRED,
                'Path to a file that should be converted to another format.',
            )
            ->addArgument('output-file', InputArgument::REQUIRED, 'Path where converted file should be saved.')
            ->addOption(
                'input-file-format',
                null,
                InputArgument::OPTIONAL,
                'File format. When not set file format is guessed from input file path extension',
                null,
            )
            ->addOption(
                'input-file-batch-size',
                null,
                InputOption::VALUE_REQUIRED,
                'Number of rows processed in one batch after reading, when set to -1 whole dataset will be displayed at once',
                self::DEFAULT_BATCH_SIZE,
            )
            ->addOption(
                'input-file-limit',
                null,
                InputOption::VALUE_REQUIRED,
                'Limit number of rows read from the file.',
                null,
            )
            ->addOption(
                'input-file-offset',
                null,
                InputOption::VALUE_REQUIRED,
                'Number of rows to skip before starting to read data',
                null,
            )
            ->addOption(
                'output-file-format',
                null,
                InputArgument::OPTIONAL,
                'File format. When not set file format is guessed from output file path extension',
                null,
            )
            ->addOption(
                'output-overwrite',
                null,
                InputOption::VALUE_OPTIONAL,
                'When set output file will be overwritten if exists',
            )
            ->addOption(
                'analyze',
                null,
                InputOption::VALUE_OPTIONAL,
                'Collect processing statistics and print them.',
                false,
            );

        (new ConfigOption('config'))->configure($this);
        (new JSONInputOptions())->configure($this);
        (new JSONOutputOptions())->configure($this);
        (new CSVInputOptions())->configure($this);
        (new CSVOutputOptions())->configure($this);
        (new ExcelInputOptions())->configure($this);
        (new XMLInputOptions())->configure($this);
        (new XMLOutputOptions())->configure($this);
        (new ParquetInputOptions())->configure($this);
        (new SchemaInferenceOptions())->configure($this);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        if (
            $this->flowConfig === null
            || $this->inputFile === null
            || $this->inputFileFormat === null
            || $this->outputFile === null
            || $this->outputFileFormat === null
        ) {
            throw new RuntimeException('Command not properly initialized.');
        }

        $style = new SymfonyStyle($input, $output);

        $extractor = (new ExtractorFactory($this->inputFile, $this->inputFileFormat))->get($input);

        if (!(new SchemaInferenceOptions())->apply($extractor, $input, $style)) {
            return Command::FAILURE;
        }

        $df = df($this->flowConfig)->read($extractor);

        $batchSize = option_int('input-file-batch-size', $input, self::DEFAULT_BATCH_SIZE);

        if ($batchSize <= 0) {
            $style->error('Batch size must be greater than 0.');

            return Command::FAILURE;
        }

        $df->batchSize($batchSize);

        $limit = option_int_nullable('input-file-limit', $input);

        if ($limit !== null && $limit > 0) {
            $df->limit($limit);
        }

        $offset = option_int_nullable('input-file-offset', $input);

        if ($offset !== null && $offset > 0) {
            $df->offset($offset);
        }

        $loader = (new LoaderFactory($this->outputFile, $this->outputFileFormat))->get($input);

        if (option_bool('output-overwrite', $input)) {
            $loader->saveMode(overwrite());
        }

        $report = $df->write($loader)->run(analyze: option_bool('analyze', $input));

        $style->success('File has been converted.');
        $style->note('File has been saved to: ' . $this->outputFile->uri());

        if ($report !== null) {
            $style->writeln(
                'Total Processed Rows: <info>' . number_format($report->statistics()->totalRows()) . '</info>',
            );
        }

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output): void
    {
        $this->flowConfig = (new ConfigOption('config'))->get($input);
        $this->inputFile = (new FilePathArgument('input-file'))->getExisting($input);
        $this->outputFile = (new FilePathArgument('output-file'))->get($input);
        $this->inputFileFormat = (new FileFormatOption($this->inputFile, 'input-file-format'))->get($input);
        $this->outputFileFormat = (new FileFormatOption($this->outputFile, 'output-file-format'))->get($input);
    }
}
