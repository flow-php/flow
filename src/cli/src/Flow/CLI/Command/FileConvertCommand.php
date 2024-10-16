<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use function Flow\CLI\{option_bool, option_int, option_int_nullable};
use function Flow\ETL\DSL\{df, overwrite};
use Flow\CLI\Arguments\FilePathArgument;
use Flow\CLI\Command\Traits\{CSVOptions, ConfigOptions, JSONOptions, ParquetOptions, XMLOptions};
use Flow\CLI\Factory\{ExtractorFactory, LoaderFactory};
use Flow\CLI\Options\{ConfigOption, FileFormat, FileFormatOption};
use Flow\ETL\{Config};
use Flow\Filesystem\Path;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

final class FileConvertCommand extends Command
{
    use ConfigOptions;
    use CSVOptions;
    use JSONOptions;
    use ParquetOptions;
    use XMLOptions;

    private const DEFAULT_BATCH_SIZE = 100;

    private ?Config $flowConfig = null;

    private ?Path $inputFile = null;

    private ?FileFormat $inputFileFormat = null;

    private ?Path $outputFile = null;

    private ?FileFormat $outputFileFormat = null;

    public function configure() : void
    {
        $this
            ->setName('file:read')
            ->setDescription('Read data from a file.')
            ->addArgument('input-file', InputArgument::REQUIRED, 'Path to a file that should be converted to another format.')
            ->addArgument('output-file', InputArgument::REQUIRED, 'Path where converted file should be saved.')
            ->addOption('input-file-format', null, InputArgument::OPTIONAL, 'File format. When not set file format is guessed from input file path extension', null)
            ->addOption('input-file-batch-size', null, InputOption::VALUE_REQUIRED, 'Number of rows that are going to be read and displayed in one batch, when set to -1 whole dataset will be displayed at once', self::DEFAULT_BATCH_SIZE)
            ->addOption('input-file-limit', null, InputOption::VALUE_REQUIRED, 'Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed', null)
            ->addOption('output-file-format', null, InputArgument::OPTIONAL, 'File format. When not set file format is guessed from output file path extension', null)
            ->addOption('output-overwrite', null, InputOption::VALUE_OPTIONAL, 'When set output file will be overwritten if exists')
            ->addOption('schema-auto-cast', null, InputOption::VALUE_OPTIONAL, 'When set Flow will try to automatically cast values to more precise data types, for example datetime strings will be casted to datetime type', false)
            ->addOption('analyze', null, InputOption::VALUE_OPTIONAL, 'Collect processing statistics and print them.', false);

        $this->addConfigOptions($this);
        $this->addJSONInputOptions($this);
        $this->addJSONOutputOptions($this);
        $this->addCSVInputOptions($this);
        $this->addCSOutputOptions($this);
        $this->addXMLInputOptions($this);
        $this->addXMLOutputOptions($this);
        $this->addParquetInputOptions($this);
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);

        $df = df($this->flowConfig)->read((new ExtractorFactory($this->inputFile, $this->inputFileFormat))->get($input));

        $batchSize = option_int('input-file-batch-size', $input, self::DEFAULT_BATCH_SIZE);

        if ($batchSize <= 0) {
            $style->error('Batch size must be greater than 0.');

            return Command::FAILURE;
        }

        $df->batchSize($batchSize);

        if (option_bool('schema-auto-cast', $input)) {
            $df->autoCast();
        }

        $limit = option_int_nullable('input-file-limit', $input);

        if ($limit !== null && $limit > 0) {
            $df->limit($limit);
        }

        $overwrite = option_bool('output-overwrite', $input);

        if ($overwrite) {
            $df->saveMode(overwrite());
        }

        $report = $df->write((new LoaderFactory($this->outputFile, $this->outputFileFormat))->get($input))
            ->run(analyze: option_bool('analyze', $input));

        $style->success('File has been converted.');
        $style->note('File has been saved to: ' . $this->outputFile->uri());

        if ($report !== null) {
            $style->writeln('Total Processed Rows: <info>' . \number_format($report->statistics()->totalRows()) . '</info>');
        }

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output) : void
    {
        $this->flowConfig = (new ConfigOption('config'))->get($input);
        $this->inputFile = (new FilePathArgument('input-file'))->getExisting($input, $this->flowConfig);
        $this->outputFile = (new FilePathArgument('output-file'))->get($input);
        $this->inputFileFormat = (new FileFormatOption($this->inputFile, 'input-file-format'))->get($input);
        $this->outputFileFormat = (new FileFormatOption($this->outputFile, 'output-file-format'))->get($input);
    }
}
